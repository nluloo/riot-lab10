using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using Shared.Cluster;
using Shared.Events;

namespace Nodes;

/// <summary>
/// Узел распределённой системы: свой TCP-слушатель, своё состояние AVAILABLE/FAILED, роль
/// (следует из того, кого узел признал лидером) и свой взгляд на соседей по последним heartbeat.
/// Отказ моделируется молчанием: FAILED-узел принимает соединение и не отвечает — сосед ловит таймаут.
/// </summary>
public class ClusterNode : IDisposable
{
    private const int ConnectTimeoutMs = 500;
    private const int ReplyTimeoutMs = 1000;
    private const int VoteTimeoutMs = 1500;

    private readonly Member _self;
    private readonly List<Member> _members;
    private readonly EventJournal _journal;
    private readonly TcpListener _listener;
    private readonly CancellationTokenSource _cts = new();

    /// Выборы и голосование идут строго по одному: два перекрёстных раунда признали бы двух лидеров.
    private readonly SemaphoreSlim _decisionLock = new(1, 1);
    private readonly object _stateLock = new();

    /// Последнее известное состояние каждого соседа (без самого узла) — локальная копия взгляда на кластер.
    private readonly Dictionary<int, NodeViewMessage> _views = new();

    private int _leaderId;
    private long _committedVersion;
    private string _lastProposal = string.Empty;

    public ClusterNode(Member self, List<Member> members, EventJournal journal)
    {
        _self = self;
        _members = members;
        _journal = journal;
        _listener = new TcpListener(IPAddress.Any, self.Port);

        foreach (Member member in members.Where(other => other.Id != self.Id))
        {
            _views[member.Id] = new NodeViewMessage
            {
                Id = member.Id,
                Host = member.Host,
                Port = member.Port,
                Status = ClusterStates.NoResponse
            };
        }
    }

    public int Id => _self.Id;

    public string Name => _self.Name;

    public string Status { get; private set; } = ClusterStates.Available;

    /// Роль не хранится отдельно — она вычисляется из признанного лидера, поэтому второй Leader невозможён.
    public string Role => LeaderId == Id ? ClusterStates.Leader : ClusterStates.Follower;

    public int LeaderId { get { lock (_stateLock) { return _leaderId; } } }

    public long CommittedVersion { get { lock (_stateLock) { return _committedVersion; } } }

    public int TotalNodes => _members.Count;

    public int Majority => ClusterRules.Majority(TotalNodes);

    public string LastProposal { get { lock (_stateLock) { return _lastProposal; } } }

    public void Start()
    {
        _listener.Start();
        _journal.Write(Name, $"START, слушаю {_self.Endpoint}, участников {TotalNodes}, большинство {Majority}");

        _ = Task.Run(() => AcceptLoopAsync(_cts.Token));
    }

    public void StartHeartbeat(int intervalMs)
    {
        _journal.Write(Name, $"heartbeat-опрос соседей каждые {intervalMs} мс");
        _ = Task.Run(() => HeartbeatLoopAsync(intervalMs, _cts.Token));
    }

    /// Раунд heartbeat: опросить соседей, заметить отказ, при отсутствии живого лидера выбрать новый.
    public async Task<List<NodeViewMessage>> HeartbeatRoundAsync(string reason)
    {
        if (Status == ClusterStates.Failed)
        {
            return new List<NodeViewMessage>();
        }

        ViewsRound round = await QueryViewsRoundAsync();
        List<NodeViewMessage> views = round.Views;

        // Состояние соседа печатается только когда оно изменилось: иначе отказ узла попадал бы
        // в журнал каждым раундом и лог стало бы невозможно читать.
        foreach (NodeViewMessage view in round.Changed)
        {
            if (view.Status == ClusterStates.NoResponse)
            {
                _journal.Write(Name, $"Node {view.Id} -> HEARTBEAT TIMEOUT ({reason}): ответа нет за {ReplyTimeoutMs} мс");
                _journal.Write(Name, $"Node {view.Id} -> FAILED");
            }
            else
            {
                _journal.Write(Name, $"Node {view.Id} -> {view.Status} ({reason})");
            }
        }

        int leader = LeaderId;
        bool leaderAlive = leader != 0 && views.Any(view => view.Id == leader && view.Status == ClusterStates.Available);

        if (!leaderAlive)
        {
            await EnsureLeaderAsync(leader == 0 ? "стартовый опрос" : $"лидер Node {leader} недоступен");
        }

        return views;
    }

    /// Выбор лидера: правило — наибольший id среди доступных, результат подтверждается большинством кластера.
    public async Task EnsureLeaderAsync(string reason)
    {
        if (Status == ClusterStates.Failed)
        {
            return;
        }

        await _decisionLock.WaitAsync(_cts.Token);

        try
        {
            int current = LeaderId;

            if (current != 0 && IsKnownAvailable(current))
            {
                _journal.Write(Name, $"Election skipped ({reason}): лидер Node {current} жив, повторный выбор не требуется");
                return;
            }

            _journal.Write(Name, $"Election started ({reason})");

            List<NodeViewMessage> views = (await QueryViewsRoundAsync()).Views;

            foreach (NodeViewMessage view in views.OrderBy(view => view.Id))
            {
                _journal.Write(Name, $"Node {view.Id} -> {view.Status}");
            }

            int candidate = ClusterRules.PickLeader(views);

            if (candidate == 0)
            {
                SetLeader(0);
                _journal.Write(Name, "ELECTION FAILED: доступных узлов нет, лидера выбрать некому");
                return;
            }

            foreach (NodeViewMessage dead in views.Where(view => view.Status != ClusterStates.Available))
            {
                _journal.Write(Name, $"Node {dead.Id} excluded");
            }

            List<int> participants = views
                .Where(view => view.Status == ClusterStates.Available)
                .Select(view => view.Id)
                .ToList();

            // Свой ACK узел засчитывает сам: он участник раунда и признаёт собственного кандидата.
            int acknowledgements = 1;

            foreach (int peerId in participants.Where(peerId => peerId != Id))
            {
                PubSubMessage? reply = await SendAsync(peerId, ClusterOperations.Elect,
                    new ElectMessage { CandidateId = candidate, FromId = Id }, ReplyTimeoutMs);

                ElectAckMessage? ack = reply == null ? null : PubSubJson.Payload<ElectAckMessage>(reply);

                if (ack == null)
                {
                    _journal.Write(Name, $"Node {peerId} -> NO RESPONSE на elect");
                }
                else if (ack.Accept)
                {
                    acknowledgements++;
                    _journal.Write(Name, $"Node {peerId} -> ACK: Node {candidate} проходит");
                }
                else
                {
                    _journal.Write(Name, $"Node {peerId} -> отказ признать Node {candidate}: {ack.Message}");
                }
            }

            if (ClusterRules.IsElectionConfirmed(acknowledgements, TotalNodes))
            {
                SetLeader(candidate);
                _journal.Write(Name, $"Node {candidate} elected as LEADER (acks = {acknowledgements}, majority = {Majority})");
                _journal.Write(Name, candidate == Id
                    ? $"{Name} -> ROLE {ClusterStates.Leader}"
                    : $"{Name} -> ROLE {ClusterStates.Follower}");

                // Подтверждение рассылают после подсчёта: до него узлы лишь согласились, а лидера
                // не признавали. Иначе два неповторившихся раунда оставили бы в кластере двух лидеров.
                await SendParallelAsync(
                    participants
                        .Where(peerId => peerId != Id)
                        .Select(peerId => _members.First(member => member.Id == peerId)),
                    ClusterOperations.Elect,
                    new ElectMessage { CandidateId = candidate, FromId = Id, Confirmed = true },
                    ReplyTimeoutMs);
            }
            else
            {
                SetLeader(0);
                _journal.Write(Name,
                    $"ELECTION NOT CONFIRMED: acks = {acknowledgements}, majority = {Majority} — лидера нет, решения не принимаются");
            }
        }
        finally
        {
            _decisionLock.Release();
        }
    }

    /// Предложение значения и подсчёт голосов. Проводит только лидер — отсюда вызывается сервером.
    public async Task<ConsensusResultMessage> VoteAsync(long version, string value)
    {
        await _decisionLock.WaitAsync(_cts.Token);

        try
        {
            if (LeaderId != Id)
            {
                _journal.Write(Name, $"propose Version={version} отклонён: {Name} не лидер, признан Node {LeaderId}");

                return new ConsensusResultMessage
                {
                    Achieved = false,
                    Verdict = "CONSENSUS NOT ACHIEVED",
                    TotalNodes = TotalNodes,
                    Majority = Majority,
                    Version = version,
                    Value = value,
                    Message = $"{Name} не лидер: признан Node {LeaderId}"
                };
            }

            _journal.Write(Name, $"Leader {Name} -> PROPOSE Version={version}: {value}");

            List<(Member Member, PubSubMessage? Reply)> replies = await SendParallelAsync(
                _members.Where(member => member.Id != Id),
                ClusterOperations.Vote,
                new ProposeMessage { Version = version, Value = value },
                VoteTimeoutMs);

            var votes = new List<string> { $"{Name} -> YES (голос лидера)" };
            var responsive = new List<int>();
            int yes = 1;
            int no = 0;
            int unanswered = 0;

            foreach ((Member member, PubSubMessage? reply) in replies)
            {
                VoteMessage? vote = reply == null ? null : PubSubJson.Payload<VoteMessage>(reply);

                if (vote == null)
                {
                    unanswered++;
                    votes.Add($"Node {member.Id} -> NO RESPONSE");
                }
                else
                {
                    responsive.Add(member.Id);
                    votes.Add($"Node {member.Id} -> {vote.Vote}: {vote.Message}");

                    if (vote.Accept)
                    {
                        yes++;
                    }
                    else
                    {
                        no++;
                    }
                }
            }

            ConsensusResultMessage result = ClusterRules.Tally(yes, no, unanswered, TotalNodes, version, value, Id);

            foreach (string line in votes)
            {
                _journal.Write(Name, line);
            }

            _journal.Write(Name, $"YES = {yes}, NO = {no}, без ответа = {unanswered}");
            _journal.Write(Name, $"Majority = {Majority} (доступно узлов {TotalNodes - unanswered} из {TotalNodes})");
            _journal.Write(Name, result.Verdict);

            lock (_stateLock)
            {
                _lastProposal = $"Version={version} ({value}) -> {result.Verdict}, YES={yes}, Majority={Majority}";
            }

            if (result.Achieved)
            {
                await CommitAsync(version, value, responsive);
            }
            else
            {
                _journal.Write(Name, "Решение не принято: изменение не фиксируется ни на одном узле");
            }

            result.Votes = votes;

            return result;
        }
        finally
        {
            _decisionLock.Release();
        }
    }

    /// Согласованное значение передаётся всем, кто ответил: у них версия становится зафиксированной.
    private async Task CommitAsync(long version, string value, List<int> responsive)
    {
        await SendParallelAsync(
            _members.Where(member => responsive.Contains(member.Id)),
            ClusterOperations.Commit,
            new ProposeMessage { Version = version, Value = value },
            VoteTimeoutMs);

        lock (_stateLock)
        {
            if (version > _committedVersion)
            {
                _committedVersion = version;
            }
        }

        var applied = new List<string> { Name };
        applied.AddRange(_members.Where(member => responsive.Contains(member.Id)).Select(member => member.Name));

        _journal.Write(Name, $"COMMIT Version={version} зафиксирована узлами {string.Join(", ", applied.OrderBy(name => name))}");
    }

    /// Раунд опроса state у всех соседей параллельно. Молчание — это NO RESPONSE, а не исключение.
    private async Task<ViewsRound> QueryViewsRoundAsync()
    {
        List<(Member Member, PubSubMessage? Reply)> replies = await SendParallelAsync(
            _members.Where(member => member.Id != Id),
            ClusterOperations.State,
            new { },
            ReplyTimeoutMs);

        var views = new List<NodeViewMessage> { OwnView() };
        var changed = new List<NodeViewMessage>();

        lock (_stateLock)
        {
            foreach ((Member member, PubSubMessage? reply) in replies)
            {
                NodeViewMessage? answer = reply == null ? null : PubSubJson.Payload<NodeViewMessage>(reply);

                _views.TryGetValue(member.Id, out NodeViewMessage? previous);

                NodeViewMessage view = new()
                {
                    Id = member.Id,
                    Host = member.Host,
                    Port = member.Port,
                    Status = answer?.Status ?? ClusterStates.NoResponse,
                    Role = answer?.Role ?? string.Empty,
                    CommittedVersion = answer?.CommittedVersion ?? previous?.CommittedVersion ?? 0
                };

                if (previous == null || previous.Status != view.Status)
                {
                    changed.Add(view);
                }

                _views[member.Id] = view;
                views.Add(view);
            }
        }

        return new ViewsRound(views, changed);
    }

    private sealed record ViewsRound(List<NodeViewMessage> Views, List<NodeViewMessage> Changed);

    public NodeViewMessage OwnView() => new()
    {
        Id = Id,
        Host = _self.Host,
        Port = _self.Port,
        Status = Status,
        Role = Role,
        CommittedVersion = CommittedVersion
    };

    /// Снимок: своё состояние плюс последнее известное о соседях.
    public List<NodeViewMessage> KnownViews()
    {
        lock (_stateLock)
        {
            List<NodeViewMessage> views = _views.Values.ToList();
            views.Add(OwnView());

            return views.OrderBy(view => view.Id).ToList();
        }
    }

    public ClusterInfoMessage Info() => new()
    {
        NodeId = Id,
        Status = Status,
        Role = Role,
        LeaderId = LeaderId,
        TotalNodes = TotalNodes,
        Majority = Majority,
        CommittedVersion = CommittedVersion,
        Nodes = KnownViews(),
        LastProposal = LastProposal,
        Journal = _journal.Recent(25)
    };

    /// Отказ узла: состояние меняется локально, на сообщения кластера узел перестаёт отвечать.
    /// Команду restore это не касается — иначе остановленный узел нельзя было бы поднять.
    public void MarkFailed(string reason)
    {
        if (Status == ClusterStates.Failed)
        {
            _journal.Write(Name, $"FAIL уже установлен ({reason}) — состояние не меняется");
            return;
        }

        Status = ClusterStates.Failed;

        lock (_stateLock)
        {
            if (_leaderId == Id)
            {
                _leaderId = 0;
            }
        }

        _journal.Write(Name, $"{Name} -> FAILED ({reason})");
        _journal.Write(Name, "NO ANSWER на heartbeat, state, elect, propose, vote, commit, check");
    }

    public async Task RestoreAsync()
    {
        if (Status == ClusterStates.Available)
        {
            _journal.Write(Name, "RESTORE: узел и так доступен");
            return;
        }

        Status = ClusterStates.Available;

        _journal.Write(Name, $"{Name} -> AVAILABLE (восстановлен после отказа)");
        await HeartbeatRoundAsync("восстановление узла");
    }

    private void SetLeader(int candidateId)
    {
        lock (_stateLock)
        {
            _leaderId = candidateId;
        }
    }

    private bool IsKnownAvailable(int peerId)
    {
        if (peerId == Id)
        {
            return Status == ClusterStates.Available;
        }

        lock (_stateLock)
        {
            return _views.TryGetValue(peerId, out NodeViewMessage? view) && view.Status == ClusterStates.Available;
        }
    }

    private async Task HeartbeatLoopAsync(int intervalMs, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(intervalMs, cancellationToken);
                await HeartbeatRoundAsync("heartbeat");
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (Exception ex)
            {
                _journal.Write(Name, $"ошибка heartbeat: {ex.GetType().Name}: {ex.Message}");
            }
        }
    }

    private async Task AcceptLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            TcpClient client;

            try
            {
                client = await _listener.AcceptTcpClientAsync(cancellationToken);
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (SocketException)
            {
                return;
            }

            // Каждое входящее сообщение обрабатывается своей задачей: узел обязан выдержать
            // несколько одновременных elect/propose от соседей.
            _ = Task.Run(() => HandleConnectionAsync(client, cancellationToken), cancellationToken);
        }
    }

    private async Task HandleConnectionAsync(TcpClient client, CancellationToken cancellationToken)
    {
        using (client)
        {
            using LineConnection connection = LineConnection.FromAccepted(client);

            PubSubMessage? message;

            try
            {
                string? line = await connection.ReadLineAsync(cancellationToken);

                if (line == null)
                {
                    return;
                }

                message = PubSubJson.Deserialize<PubSubMessage>(line);
            }
            catch (JsonException)
            {
                _journal.Write(Name, "ошибочное сообщение: JSON не разобран, ответ не отправлен");
                return;
            }
            catch (Exception ex) when (ex is IOException or SocketException or OperationCanceledException)
            {
                return;
            }

            if (message == null || string.IsNullOrWhiteSpace(message.Operation))
            {
                _journal.Write(Name, "ошибочное сообщение: нет имени операции — отвечено ошибкой");
                await ReplyAsync(connection, new VoteMessage { Accept = false, Vote = "NO", Message = "сообщение без операции" });
                return;
            }

            object? answer = await DispatchAsync(message);

            if (answer == null)
            {
                _journal.Write(Name, $"{Name} молчит на '{message.Operation}': узел в состоянии FAILED");
                return;
            }

            await ReplyAsync(connection, answer);
        }
    }

    /// null — узел намеренно не отвечает (симуляция отказа). Остальные операции всегда дают ответ.
    private async Task<object?> DispatchAsync(PubSubMessage message)
    {
        bool clusterMessage = message.Operation is ClusterOperations.Heartbeat
            or ClusterOperations.State
            or ClusterOperations.Elect
            or ClusterOperations.Propose
            or ClusterOperations.Vote
            or ClusterOperations.Commit
            or ClusterOperations.Check;

        if (clusterMessage && Status == ClusterStates.Failed)
        {
            return null;
        }

        switch (message.Operation)
        {
            case ClusterOperations.Heartbeat:
            case ClusterOperations.State:
                return OwnView();

            case ClusterOperations.Check:
                await HeartbeatRoundAsync("проверка по команде");
                return Info();

            case ClusterOperations.Elect:
                return AcceptElection(PubSubJson.Payload<ElectMessage>(message));

            // propose — это команда ЛИДЕРУ провести голосование, vote — запрос соседу проголосовать.
            case ClusterOperations.Propose:
            {
                ProposeMessage? proposal = PubSubJson.Payload<ProposeMessage>(message);

                if (proposal == null || string.IsNullOrWhiteSpace(proposal.Value) || proposal.Version <= 0)
                {
                    _journal.Write(Name, "ошибочное сообщение propose: нет значения или версии — голосование не проведено");

                    return new ConsensusResultMessage
                    {
                        Achieved = false,
                        Verdict = "CONSENSUS NOT ACHIEVED",
                        TotalNodes = TotalNodes,
                        Majority = Majority,
                        Message = "предложение без значения или версии"
                    };
                }

                return await VoteAsync(proposal.Version, proposal.Value);
            }

            case ClusterOperations.Vote:
                return CastVote(PubSubJson.Payload<ProposeMessage>(message));

            case ClusterOperations.Commit:
                return ApplyCommit(PubSubJson.Payload<ProposeMessage>(message));

            case ClusterOperations.Info:
                return Info();

            case ClusterOperations.Fail:
                MarkFailed("команда отказа от сервера");

                return new NodeControlMessage
                {
                    NodeId = Id,
                    Action = ClusterOperations.Fail,
                    Success = true,
                    Status = Status,
                    Message = $"{Name} переведён в FAILED"
                };

            case ClusterOperations.Restore:
                await RestoreAsync();

                return new NodeControlMessage
                {
                    NodeId = Id,
                    Action = ClusterOperations.Restore,
                    Success = true,
                    Status = Status,
                    Message = $"{Name} восстановлен"
                };

            default:
                _journal.Write(Name, $"неизвестная операция '{message.Operation}' — отвечено ошибкой");

                return new VoteMessage
                {
                    Accept = false,
                    Vote = "NO",
                    Message = $"неизвестная операция {message.Operation}"
                };
        }
    }

    private object AcceptElection(ElectMessage? request)
    {
        if (request == null || request.CandidateId <= 0)
        {
            _journal.Write(Name, "ошибочное сообщение elect: нет id кандидата — лидер не признан");

            return new ElectAckMessage
            {
                Accept = false,
                LeaderId = LeaderId,
                Message = "объявление без id кандидата"
            };
        }

        int current = LeaderId;

        // Более слабый кандидат не смещает уже признанного ЛИДЕРСКОГО узла, но смещает мёртвого:
        // иначе после отказа Node 5 остальные вечно ждали бы его и перевыборы не собрали бы большинства.
        if (current != 0 && request.CandidateId < current && IsKnownAvailable(current))
        {
            _journal.Write(Name, $"отказ признать Node {request.CandidateId}: уже признан Node {current}");

            return new ElectAckMessage
            {
                Accept = false,
                LeaderId = current,
                Message = $"уже признан Node {current}"
            };
        }

        // Предварительное объявление — только согласие. Лидера признают после подтверждения
        // выборов большинством: признавать каждого, кто спросил первым, значило бы двух лидеров.
        if (!request.Confirmed)
        {
            return new ElectAckMessage
            {
                Accept = true,
                LeaderId = request.CandidateId,
                Message = "согласен, жду подтверждения большинством"
            };
        }

        SetLeader(request.CandidateId);

        _journal.Write(Name, $"Node {request.CandidateId} признан лидером (объявление от Node {request.FromId}), {Name} -> {Role}");

        return new ElectAckMessage { Accept = true, LeaderId = request.CandidateId, Message = "лидер признан" };
    }

    private object CastVote(ProposeMessage? proposal)
    {
        if (proposal == null || string.IsNullOrWhiteSpace(proposal.Value))
        {
            _journal.Write(Name, "ошибочное сообщение propose: нет значения — голос NO");

            return new VoteMessage { Accept = false, Vote = "NO", Message = "предложение без значения" };
        }

        long committed = CommittedVersion;
        bool fresh = proposal.Version > committed;

        _journal.Write(Name, $"Node {Id} голосует {(fresh ? "YES" : "NO")}: предложено Version={proposal.Version}, зафиксировано {committed}");

        return new VoteMessage
        {
            Accept = fresh,
            Vote = fresh ? "YES" : "NO",
            Message = fresh
                ? "значение свежее зафиксированного"
                : $"такое или более новое решение уже зафиксировано (Version={committed})"
        };
    }

    private object ApplyCommit(ProposeMessage? proposal)
    {
        if (proposal == null || proposal.Version <= 0)
        {
            return new VoteMessage { Accept = false, Vote = "NO", Message = "commit без корректной версии" };
        }

        lock (_stateLock)
        {
            if (proposal.Version > _committedVersion)
            {
                _committedVersion = proposal.Version;
            }
        }

        _journal.Write(Name, $"{Name} -> COMMIT Version={proposal.Version}");

        return new VoteMessage { Accept = true, Vote = "YES", Message = $"зафиксировано Version={proposal.Version}" };
    }

    private async Task ReplyAsync(LineConnection connection, object answer)
    {
        try
        {
            await connection.SendAsync(PubSubJson.Message(ClusterOperations.Result, answer), _cts.Token);
        }
        catch (Exception ex) when (ex is IOException or SocketException or PubSubTransportException or OperationCanceledException)
        {
            _journal.Write(Name, $"не удалось отправить ответ: {ex.Message}");
        }
    }

    private Task<List<(Member Member, PubSubMessage? Reply)>> SendParallelAsync(
        IEnumerable<Member> targets,
        string operation,
        object data,
        int timeoutMs) => ClusterTransport.RequestManyAsync(targets, operation, data, ConnectTimeoutMs, timeoutMs, _cts.Token);

    private async Task<PubSubMessage?> SendAsync(int peerId, string operation, object data, int timeoutMs)
    {
        Member? member = _members.FirstOrDefault(other => other.Id == peerId);

        return member == null
            ? null
            : await ClusterTransport.RequestAsync(member, operation, data, ConnectTimeoutMs, timeoutMs, _cts.Token);
    }

    public void Dispose()
    {
        _cts.Cancel();
        _listener.Stop();
        _decisionLock.Dispose();
        _cts.Dispose();
    }
}
