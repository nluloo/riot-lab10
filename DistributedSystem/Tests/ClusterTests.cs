using Nodes;
using Shared.Cluster;
using Shared.Events;

namespace Tests;

/// <summary>
/// Кластер из ЛР №7: правила большинства считаются отдельно (Part 2), выборы и голосование
/// идут на пяти настоящих узлах в одном процессе на портах 6101…6105.
/// </summary>
public class ClusterTests : IDisposable
{
    private const int BasePort = 6100;

    private readonly EventJournal _journal = new();
    private readonly List<Member> _members = Enumerable.Range(1, 5)
        .Select(id => new Member(id, "127.0.0.1", BasePort + id))
        .ToList();

    private readonly List<ClusterNode> _nodes = new();

    public ClusterTests()
    {
        foreach (Member member in _members)
        {
            ClusterNode node = new(member, _members, _journal);
            node.Start();
            _nodes.Add(node);
        }
    }

    public void Dispose()
    {
        foreach (ClusterNode node in _nodes)
        {
            node.Dispose();
        }
    }

    // Часть 2: большинство считается от полного состава кластера, а не от числа живых узлов.
    [Theory]
    [InlineData(1, 1)]
    [InlineData(2, 2)]
    [InlineData(3, 2)]
    [InlineData(4, 3)]
    [InlineData(5, 3)]
    public void Majority_IsCountedFromFullCluster(int totalNodes, int expected)
    {
        Assert.Equal(expected, ClusterRules.Majority(totalNodes));
    }

    [Fact]
    public void PickLeader_TakesMaxIdAmongAvailable()
    {
        List<NodeViewMessage> views = new()
        {
            View(1, ClusterStates.Available),
            View(5, ClusterStates.Failed),
            View(4, ClusterStates.Available)
        };

        Assert.Equal(4, ClusterRules.PickLeader(views));
    }

    [Fact]
    public void Tally_MajorityOfYes_AchievesConsensus()
    {
        ConsensusResultMessage result = ClusterRules.Tally(3, 2, 0, 5, 7, "RentalStatus = Active", 5);

        Assert.True(result.Achieved);
        Assert.Equal("CONSENSUS ACHIEVED", result.Verdict);
    }

    // Требование методички: голоса поровну — это НЕ консенсус.
    [Fact]
    public void Tally_TieAndShortMajority_DoNotAchieveConsensus()
    {
        Assert.False(ClusterRules.Tally(2, 2, 1, 5, 7, "value", 5).Achieved);
        Assert.False(ClusterRules.Tally(2, 0, 3, 5, 7, "value", 5).Achieved);
        Assert.Equal("CONSENSUS NOT ACHIEVED", ClusterRules.Tally(2, 0, 3, 5, 7, "value", 5).Verdict);
    }

    [Fact]
    public void MemberParse_RejectsMalformedSetting()
    {
        Assert.Throws<FormatException>(() => Member.Parse("1=127.0.0.1"));
        Assert.Throws<FormatException>(() => Member.Parse("1=127.0.0.1:6001,1=127.0.0.1:6002"));
        Assert.Equal(5, Member.Parse(null).Count);
    }

    // Часть 1 и 2: выборы при старте — лидером становится узел с наибольшим id,
    // решение подтверждается большинством.
    [Fact]
    public async Task Election_AtStartup_AcknowledgesNodeWithMaxId()
    {
        await RoundAllAsync("тест: старт");

        Assert.All(_nodes, node => Assert.Equal(5, node.LeaderId));
        Assert.Equal("LEADER", _nodes[4].Role);
        Assert.Equal("FOLLOWER", _nodes[0].Role);
    }

    // Часть 3: отказ лидера замечают heartbeat-раундом, перевыборы дают следующий по id узел.
    [Fact]
    public async Task Reelection_AfterLeaderFailure_PicksNextMaxId()
    {
        await RoundAllAsync("тест: старт");

        _nodes[4].MarkFailed("тест: отказ лидера");
        await RoundAllAsync("тест: перевод Node 5 в FAILED");

        Assert.All(_nodes.Take(4), node => Assert.Equal(4, node.LeaderId));
        Assert.Equal(0, _nodes[4].LeaderId);
    }

    // Часть 4: консенсус достигается, когда YES сказали не меньше большинства; значение фиксируется.
    [Fact]
    public async Task Propose_WithMajorityYes_CommitsOnEveryNode()
    {
        await RoundAllAsync("тест: старт");

        ConsensusResultMessage result = await _nodes[4].VoteAsync(1, "RentalStatus = Active");

        Assert.True(result.Achieved);
        Assert.Equal(5, result.Yes);
        Assert.Equal(0, result.No);
        Assert.All(_nodes, node => Assert.Equal(1, node.CommittedVersion));
    }

    // Часть 4, ошибочный сценарий: предложение со старой версии получает NO от всех.
    [Fact]
    public async Task Propose_WithStaleVersion_IsRejected()
    {
        await RoundAllAsync("тест: старт");
        await _nodes[4].VoteAsync(1, "RentalStatus = Active");

        ConsensusResultMessage stale = await _nodes[4].VoteAsync(1, "RentalStatus = Cancelled");

        Assert.False(stale.Achieved);
        Assert.Equal(1, stale.Yes);
        Assert.Equal(4, stale.No);
    }

    // Часть 5 + Эксперимент 4: при трёх отказах большинство не собирается, лидера нет.
    [Fact]
    public async Task ThreeFailures_LeaveNoConfirmedLeader()
    {
        await RoundAllAsync("тест: старт");

        foreach (int index in new[] { 4, 3, 2 })
        {
            _nodes[index].MarkFailed("тест: отказ");
        }

        await RoundAllAsync("тест: отказ Node 3, 4, 5");

        Assert.All(_nodes.Take(2), node => Assert.Equal(0, node.LeaderId));
        Assert.All(_nodes.Take(2), node => Assert.Equal(ClusterStates.Available, node.Status));
    }

    // Эксперимент 3: трёх живых узлов хватает ровно на большинство — решение принимается.
    [Fact]
    public async Task Proposal_WithThreeAvailableNodes_AchievesMajority()
    {
        await RoundAllAsync("тест: старт");

        foreach (int index in new[] { 4, 3 })
        {
            _nodes[index].MarkFailed("тест: отказ");
        }

        await RoundAllAsync("тест: отказ Node 4 и Node 5");

        Assert.Equal(3, _nodes[2].LeaderId);

        ConsensusResultMessage result = await _nodes[2].VoteAsync(1, "RentalStatus = Active");

        Assert.True(result.Achieved);
        Assert.Equal(3, result.Yes);
        Assert.Equal(2, result.Unanswered);
    }

    // Восстановление узла: он снова отвечает, признаёт действующего лидера и добирает очередь предложений.
    [Fact]
    public async Task RestoredNode_RejoinsCurrentLeader()
    {
        await RoundAllAsync("тест: старт");
        _nodes[4].MarkFailed("тест: отказ лидера");
        await RoundAllAsync("тест: перевыборы");
        await _nodes[3].VoteAsync(1, "RentalStatus = Active");

        await _nodes[4].RestoreAsync();
        await RoundAllAsync("тест: восстановление Node 5");

        Assert.Equal(ClusterStates.Available, _nodes[4].Status);
        Assert.Equal(5, _nodes[4].LeaderId);
        Assert.All(_nodes, node => Assert.Equal(5, node.LeaderId));
    }

    private async Task RoundAllAsync(string reason)
    {
        await Task.WhenAll(_nodes
            .Where(node => node.Status == ClusterStates.Available)
            .Select(node => node.HeartbeatRoundAsync(reason)));
    }

    private static NodeViewMessage View(int id, string status) => new()
    {
        Id = id,
        Host = "127.0.0.1",
        Port = 6100 + id,
        Status = status
    };
}
