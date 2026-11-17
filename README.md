# ЛР №10 «Автоматизация сборки, тестирования и развертывания распределенного приложения с помощью CI/CD»

**Предметная область**: «Аренда автомобилей» (вариант 11, C#/.NET 8).  
**Дата реализации**: Ноябрь 2026.  
**Контекст**: Выполняется как продолжение ЛР №9. Приложение автоматизировано с использованием **Jenkins**, **Git**, **Docker** и оркестратора **Kubernetes (k3s)**.

---

## 1. Архитектура и структура CI/CD Pipeline

Конвейер непрерывной интеграции и доставки (CI/CD) охватывает полный жизненный цикл изменений: от фиксации в Git до автоматического развертывания в промышленном кластере Kubernetes.

```
┌─────────────────┐       git push       ┌────────────────────────────────┐
│  Разработчик    │ ───────────────────> │ Git-репозиторий (riot-lab10)   │
└─────────────────┘                      └──────────────┬─────────────────┘
                                                        │ Webhook / Poll SCM / Trigger
                                                        ▼
┌───────────────────────────────── Jenkins CI/CD Server (:8085) ──────────────────────────────────┐
│                                                                                                  │
│  Job: RIOT-CICD                                                                                  │
│                                                                                                  │
│  [1. Checkout]  ──>  [2. Restore]  ──>  [3. Build]  ──>  [4. Test]  ──>  [5. Package]  ──>  [6. Deploy]
│   git checkout        dotnet restore     dotnet build      dotnet test     docker build       deploy_to_k3s.py
│   ветка main          зависимости        Release           129 тестов      riot-server:tag    k3s rollout
│                                                            отчет .trx                         status
└─────────────────────────────────────────────────────────────────────────────────┬────────────────┘
                                                                                  │
                                              ┌───────────────────────────────────┘
                                              ▼
┌────────────────────────────────── Kubernetes Cluster (k3s) ──────────────────────────────────────┐
│  Узел: debian (192.168.0.193:6443)                                                               │
│                                                                                                  │
│  1. ctr images import (riot-server:tag)                                                          │
│  2. kubectl set image deployment/riot-server server=riot-server:tag                              │
│  3. Rolling Update (3 реплики): Pod-1, Pod-2, Pod-3 (Zero Downtime)                              │
│  4. Service: riot-server (ClusterIP:5000)                                                        │
│  5. Интеграция: PostgreSQL (Primary/Replica), Broker Pub/Sub (:5001), 5 узлов консенсуса        │
└──────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### Этапы пайплайна (`Jenkinsfile`):
1. **Checkout**: Получение актуальной ревизии исходного кода из Git-репозитория.
2. **Restore**: Восстановление NuGet-пакетов и зависимостей проектов решения (`dotnet restore`).
3. **Build**: Компиляция всех проектов решения в конфигурации `Release` без повторного восстановления (`dotnet build --no-restore`).
4. **Test**: Запуск полного набора из 129 Unit-, нагрузочных и интеграционных тестов с генерацией отчета о тестировании в формате Visual Studio `.trx` (`dotnet test --logger "trx"`). При сбое хотя бы одного теста пайплайн прерывается со статусом `FAILURE`.
5. **Package**: Сборка оптимизированного многоэтапного Docker-образа сервера (`docker build -t riot-server:%BUILD_NUMBER% .`).
6. **Deploy**: Доставка Docker-образа на управляющий узел k3s (`sudo k3s ctr images import`), обновление образа в `deployment/riot-server` через `kubectl set image` и контроль успешного завершения развертывания через `kubectl rollout status`.

### Структура каталогов проекта:
* `DistributedSystem/` — исходный код решения на .NET 8 (`Server`, `Client`, `Shared`, `Nodes`, `Broker`, `Subscribers`, `Tests`);
* `k8s-manifests/` — декларативные манифесты Kubernetes (`deployment.yaml`, `service.yaml`, `configmap.yaml`, `broker.yaml`, `cluster-nodes.yaml`, `postgres.yaml`);
* `k3s-config/` — файл конфигурации доступа `kubeconfig` (`k3s.yaml`) для взаимодействия с API-сервером k3s;
* `deploy_to_k3s.py` — скрипт автоматизированной доставки образа и обновления Deployment;
* `Jenkinsfile` — сценарий конвейера CI/CD (Pipeline as Code).

---

## 2. Результаты выполнения Части 1: Подготовка окружения и подключение Git к Jenkins

### Проверка инструментов окружения
* **.NET SDK**: `8.0.101` (MSBuild 17.8.3).
* **Git**: `git version 2.42.0.windows.2`.
* **Docker**: `Docker version 28.0.1, build 068a01e` (Docker Desktop Linux engine).
* **kubectl**: Client Version `v1.30.0`, Server Version `v1.37.2+k3s1`.
* **Jenkins**: `2.580.2` (порт `8085`, `JENKINS_HOME=D:\labs\sem7\riot\.temp\jenkins_home`).

### Настройка репозитория и Jenkins Job
* Репозиторий инициализирован в каталоге `labs/lab10` с веткой `main`.
* В качестве удаленного репозитория настроен локальный bare-репозиторий `D:/labs/sem7/riot/.temp/riot-repo.git`.
* Создано задание Pipeline под именем **`RIOT-CICD`**.
* В конфигурации запуска Jenkins установлен флаг `-Dhudson.plugins.git.GitSCM.ALLOW_LOCAL_CHECKOUT=true`, разрешающий чекаут из локальных каталогов файловой системы.

### Таблица 1. Результаты подготовки среды и подключения Git к Jenkins (Часть 1, Требование 5)

| № | Проверяемый параметр | Результат |
|---|---|---|
| 1 | Название Git-репозитория | `riot-lab10` (локальный upstream `riot-repo.git`) |
| 2 | URL репозитория | `D:/labs/sem7/riot/.temp/riot-repo.git` |
| 3 | Используемая ветка | `main` |
| 4 | Название Jenkins Job | `RIOT-CICD` |
| 5 | Расположение Jenkinsfile | `Jenkinsfile` (в корне репозитория) |
| 6 | Версия .NET SDK | `.NET SDK 8.0.101` (MSBuild 17.8.3) |
| 7 | Версия Git | `git version 2.42.0.windows.2` |
| 8 | Версия Docker | `Docker version 28.0.1, build 068a01e` (Docker Desktop) |
| 9 | Версия kubectl | Client: `v1.30.0`, Server (k3s): `v1.37.2+k3s1` |
| 10 | Результат первого запуска Pipeline | `FAILURE` (чекаут заблокирован встроенной политикой безопасности `GitSCM`) |
| 11 | Обнаруженные ошибки и способы устранения | Добавлен параметр `-Dhudson.plugins.git.GitSCM.ALLOW_LOCAL_CHECKOUT=true` при старте Jenkins |

---

## 3. Результаты выполнения Части 2 и 3: Continuous Integration и создание Docker-образа

### Декларативный пайплайн (`Jenkinsfile`)
```groovy
pipeline {
    agent any
    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }
        stage('Restore') {
            steps {
                bat 'dotnet restore DistributedSystem/DistributedSystem.sln'
            }
        }
        stage('Build') {
            steps {
                bat 'dotnet build DistributedSystem/DistributedSystem.sln --configuration Release --no-restore'
            }
        }
        stage('Test') {
            steps {
                bat 'dotnet test DistributedSystem/Tests/Tests.csproj --configuration Release --no-restore --logger "trx"'
            }
        }
        stage('Package') {
            steps {
                bat 'docker build -t riot-server:%BUILD_NUMBER% .'
            }
        }
        stage('Deploy') {
            steps {
                bat 'python deploy_to_k3s.py %BUILD_NUMBER%'
            }
        }
    }
}
```

### Лог успешного прогона этапов сборки и тестирования (Сборка #3)
```text
[Pipeline] { (Restore)
[Pipeline] bat
dotnet restore DistributedSystem/DistributedSystem.sln 
  Определение проектов для восстановления...
  Все проекты обновлены для восстановления.
[Pipeline] }
[Pipeline] { (Build)
[Pipeline] bat
dotnet build DistributedSystem/DistributedSystem.sln --configuration Release --no-restore 
  Shared -> ...\Shared.dll
  Nodes -> ...\Nodes.dll
  Broker -> ...\Broker.dll
  Server -> ...\Server.dll
  Tests -> ...\Tests.dll
[Pipeline] }
[Pipeline] { (Test)
[Pipeline] bat
dotnet test DistributedSystem/Tests/Tests.csproj --configuration Release --no-restore --logger "trx"
Файл результатов: ...\DistributedSystem\Tests\TestResults\kirill_DESKTOP-KB55DME_2026-11-15_11_58_40.trx
Пройден!   : не пройдено     0, пройдено   129, пропущено     0, всего   129, длительность 1 m 20 s.
[Pipeline] }
Finished: SUCCESS
```

### Моделирование ошибки тестирования (Сборка #4)
Для проверки изоляции сбоев в тест `KubernetesConfigTests.cs` было внесено намеренно ложное утверждение (`Assert.Equal(3, replicas)` при `replicas=2` в файле конфигурации).
* **Результат в Jenkins**:
  ```text
  [Pipeline] { (Test)
  [xUnit.net 00:00:03.21]     KubernetesConfigTests.DeploymentYaml_ConfiguresDesiredReplicas [FAIL]
  [xUnit.net 00:00:03.21]       Assert.Equal() Failure: Expected: 3, Actual: 2
  Пройден!   : не пройдено     1, пройдено   128, всего   129.
  ERROR: script returned exit code 1
  [Pipeline] }
  Finished: FAILURE
  ```
* **Поведение конвейера**: Сборка немедленно остановлена. Этапы `Package` и `Deploy` **не выполнялись**. Непротестированный код не попал в Docker и не был развернут в Kubernetes.
* **Восстановление**: Тест возвращен в исходное состояние, изменения зафиксированы в Git (`git commit`).

### Таблица 2. Создание Docker-образа и этап Package (Часть 3, Требование 6)

| № | Проверяемый параметр | Результат |
|---|---|---|
| 1 | Расположение Dockerfile | `DistributedSystem/Server/Dockerfile` |
| 2 | Имя Docker-образа | `riot-server` |
| 3 | Тег Docker-образа | `%BUILD_NUMBER%` (`riot-server:5`, `riot-server:6`, `riot-server:7`) |
| 4 | Номер сборки Jenkins | Сборка #5 (`SUCCESS`), #6 (`FAILURE` на Deploy), #7 (`SUCCESS`) |
| 5 | Результат выполнения docker build | Успешно: код возврата 0, образ собран и зарегистрирован |
| 6 | Наличие созданного образа | Да, подтверждено локальным Docker daemon |
| 7 | Удалось ли просмотреть информацию об образе | Да, через `docker inspect riot-server:5` (id `7e48308d1865`) |
| 8 | Создается ли образ при успешном тестировании | Да, этап `Package` выполняется строго после успешного `Test` |
| 9 | Создается ли образ при ошибке тестирования | Нет, сбой на `Test` прерывает пайплайн, `Package` пропускается |
| 10 | Обнаруженные ошибки и способы устранения | Контекст сборки скорректирован для корректного включения Shared и Server |

---

## 4. Результаты выполнения Части 4: Автоматизация развертывания в Kubernetes и итоговая проверка

### Автоматическое развертывание в k3s (Сборка #5)
Развертывание автоматизировано скриптом `deploy_to_k3s.py`:
1. Экспорт локального Docker-образа в архив `riot-server-{tag}.tar`.
2. Загрузка архива по защищенному протоколу SCP на управляющий узел k3s (`192.168.0.193`).
3. Импорт образа в хранилище containerd узла: `sudo k3s ctr images import`.
4. Обновление образа развертывания: `kubectl set image deployment/riot-server server=riot-server:{tag}`.
5. Ожидание завершения rolling update: `kubectl rollout status deployment/riot-server`.

```text
[Pipeline] { (Deploy)
[Pipeline] bat
python deploy_to_k3s.py 5 
Deploying riot-server:5 to k3s...
Saving docker image riot-server:5 -> ...\riot-server-5.tar...
Uploading to 192.168.0.193...
Importing into k3s ctr...
Import output:
 docker.io/library/riot-server:5                 saved
Updating deployment riot-server to riot-server:5...
deployment.apps/riot-server image updated
Waiting for rollout status...
Waiting for deployment "riot-server" rollout to finish: 1 out of 3 new replicas have been updated...
Waiting for deployment "riot-server" rollout to finish: 2 out of 3 new replicas have been updated...
Waiting for deployment "riot-server" rollout to finish: 1 old replicas are pending termination...
deployment "riot-server" successfully rolled out
Deployment successfully updated!
[Pipeline] }
Finished: SUCCESS
```

### Моделирование ошибки развертывания (Сборка #6)
Для проверки реакции CI/CD и Kubernetes на ошибку деплоя было указано несуществующее имя образа `riot-server:broken-tag-err` с таймаутом ожидания раскатки:
1. **Фиксация в Jenkins**:
   ```text
   SIMULATING DEPLOY ERROR: Setting deployment image to non-existent image docker.io/library/riot-server:broken-tag-err...
   deployment.apps/riot-server image updated
   Waiting for rollout status (expecting timeout/failure due to ImagePullBackOff)...
   error: timed out waiting for the condition
   subprocess.CalledProcessError: Command '['kubectl', 'rollout', 'status', ...]' returned non-zero exit status 1.
   ERROR: script returned exit code 1
   Finished: FAILURE
   ```
2. **Поведение Kubernetes**:
   ```text
   $ kubectl get pods
   NAME                           READY   STATUS         RESTARTS   AGE
   riot-server-769cf7b96f-5vjhr   1/1     Running        0          4m24s
   riot-server-769cf7b96f-kbx97   1/1     Running        0          4m25s
   riot-server-769cf7b96f-mxm2l   1/1     Running        0          4m22s
   riot-server-7db4b7b6d7-wzvf6   0/1     ErrImagePull   0          30s
   ```
   В событиях пода зафиксировано:
   `Warning Failed kubelet Failed to pull image "riot-server:broken-tag-err"`  
   `Normal BackOff kubelet Back-off pulling image`  
   `Warning Failed kubelet Error: ImagePullBackOff`  
   **Критически важный вывод**: Оркестратор Kubernetes сохранил в работе все 3 старых пода (`769cf7b96f-*`), предотвратив отказ приложения (*zero-downtime protection*).

### Проверка автоматического обновления приложения (Сборка #7)
В коде сервера (`Program.cs` и `RequestHandler.cs`) внесены изменения: обновлен вывод баннера и возвращаемое сообщение поиска авто с суффиксом `[v2.0-cicd]`.
1. Изменения зафиксированы в Git: `git commit -m "CI/CD: Update server to v2.0 with pipeline release notes"`.
2. Запущена Сборка #7. Все 6 этапов успешно выполнены.
3. Собран и развернут образ **`riot-server:7`**.
4. Поды плавно обновлены без простоя:
   ```text
   $ kubectl get pods
   riot-server-56ff74b7d7-2wnhq   1/1     Running   0          17s
   riot-server-56ff74b7d7-8s7qp   1/1     Running   0          16s
   riot-server-56ff74b7d7-b6wdg   1/1     Running   0          19s
   ```

### Проверка работоспособности сервиса через 3 операции предметной области

Доступ к приложению организован через стабильный `Service` типа `ClusterIP` с перенаправлением локального порта:
```bash
kubectl port-forward svc/riot-server 5000:5000
```

1. **Операция 1: Поиск свободных авто (`SearchCar`)**
   ```json
   {
     "success": true,
     "message": "Доступных автомобилей: 3. Данные читает Primary [v2.0-cicd].",
     "instance": "Primary",
     "data": [
       {"id": 1, "brand": "Toyota", "model": "Camry", "registrationNumber": "1234 AB-7", "dailyPrice": 120.0, "isAvailable": true},
       {"id": 2, "brand": "Volkswagen", "model": "Polo", "registrationNumber": "2345 BC-7", "dailyPrice": 80.0, "isAvailable": true},
       {"id": 3, "brand": "Kia", "model": "Rio", "registrationNumber": "3456 CD-1", "dailyPrice": 75.0, "isAvailable": true}
     ]
   }
   ```
   *Результат*: Подтверждена работа обновленной версии `[v2.0-cicd]`, читаются актуальные данные из Primary.

2. **Операция 2: Создание аренды (`CreateRental`)**
   ```json
   {
     "success": true,
     "message": "Аренда создана. Выполнен COMMIT в Primary. Номер аренды: 3.",
     "instance": "Primary",
     "events": "RentalCreated #3 (event c230e837) -> Broker-1: NO_SUBSCRIBERS",
     "consensus": "лидер Node 5: YES = 5, NO = 0, без ответа = 0, Majority = 3 -> CONSENSUS ACHIEVED",
     "data": {
       "id": 3,
       "customerId": 2,
       "carId": 1,
       "status": "Active",
       "amount": 360.0
     }
   }
   ```
   *Результат*: Успешная фиксация транзакции, кворум узлов консенсуса достигнут (5 из 5 голосов), событие опубликовано брокеру сообщений Pub/Sub.

3. **Операция 3: Возврат автомобиля (`ReturnCar`)**
   ```json
   {
     "success": true,
     "message": "Автомобиль возвращён",
     "instance": "Primary",
     "events": "CarReturned #3 (event 0df15463) -> Broker-1: NO_SUBSCRIBERS",
     "data": null
   }
   ```
   *Результат*: Статус аренды переведен в `Returned`, автомобиль вновь доступен для бронирования (`cars.is_available = true`), LSN Primary увеличен до 6.

### Таблица 3. Автоматизация развертывания в Kubernetes и итоговая проверка (Часть 4, Требования 5-8)

| № | Проверяемый параметр | Результат |
|---|---|---|
| 1 | Используемый кластер Kubernetes | k3s `v1.37.2+k3s1` (узел debian 192.168.0.193, containerd) |
| 2 | Имя Deployment | `riot-server` |
| 3 | Число реплик (желаемое / факт) | 3 / 3 (все поды в состоянии Running, 1/1 Ready) |
| 4 | Развернутый образ приложения | `riot-server:7` (образ сборки #7 с обновлением v2.0-cicd) |
| 5 | Метод доставки образа в кластер | Экспорт docker save в tar, SCP на узел debian, импорт через k3s ctr images import |
| 6 | Команда обновления Deployment | `kubectl set image deployment/riot-server server=riot-server:7` |
| 7 | Контроль процесса обновления | `kubectl rollout status deployment/riot-server` (завершено успешно) |
| 8 | Способ доступа к сервису | `Service riot-server` (ClusterIP:5000), `kubectl port-forward svc/riot-server 5000:5000` |
| 9 | Операция 1: SearchCar | Успешно: 3 доступных авто, подтверждена версия Primary [v2.0-cicd] |
| 10 | Операция 2: CreateRental | Успешно: аренда #3 создана, консенсус 5/5 узлов (кворум 3), событие брокеру Pub/Sub |
| 11 | Операция 3: ReturnCar | Успешно: автомобиль возвращен, состояние БД обновлено (Primary lsn = 6) |

### Таблица 4. Результаты моделирования ошибок в CI/CD Pipeline

| № | Параметр эксперимента | Результат |
|---|---|---|
| 1 | Тип ошибки 1 (Часть 3, п. 5) | Искусственный сбой Unit-теста (`KubernetesConfigTests.cs: Assert.Equal(3, replicas)`) |
| 2 | Этап фиксации ошибки 1 | `Test` (`dotnet test` завершился с ошибкой, сформирован `*.trx`) |
| 3 | Статус сборки и поведение 1 | Сборка #4: `FAILURE`. Этапы Package и Deploy заблокированы, кластер не затронут |
| 4 | Тип ошибки 2 (Часть 4, п. 7) | Ошибка развертывания: указание несуществующего Docker-образа `riot-server:broken-tag-err` |
| 5 | Этап фиксации ошибки 2 | `Deploy` (таймаут 15 с команды `kubectl rollout status`) |
| 6 | Статус сборки и поведение 2 | Сборка #6: `FAILURE`. В k3s новый Pod перешел в `ImagePullBackOff`, 3 старых Pod продолжали работать |
| 7 | Самовосстановление системы | Коммит корректного тега и сборка #7 восстановили статус `SUCCESS` и обновили все поды |

### Таблица 5. Сводная история прогонов Jenkins Pipeline (RIOT-CICD)

| № | Сборка / Коммит | Статус и результат |
|---|---|---|
| 1 | Сборка #1 (`57422f6`) | `FAILURE` — Checkout: локальный Git-репозиторий заблокирован политикой безопасности GitSCM |
| 2 | Сборка #2 (`57422f6`) | `SUCCESS` — Checkout: разрешен локальный checkout флагом ALLOW_LOCAL_CHECKOUT=true |
| 3 | Сборка #3 (`57422f6`) | `SUCCESS` — Checkout -> Restore -> Build -> Test (129 тестов пройдено за 1m 20s) |
| 4 | Сборка #4 (`ad06cbd`) | `FAILURE` — Сбой на этапе Test: симуляция ошибки теста; Package и Deploy заблокированы |
| 5 | Сборка #5 (`f5ca468`) | `SUCCESS` — Полный пайплайн: сборка riot-server:5, импорт в k3s, успешный rollout 3 реплик |
| 6 | Сборка #6 (`73c9f38`) | `FAILURE` — Сбой на этапе Deploy: симуляция сбоя образа, статус пода ImagePullBackOff |
| 7 | Сборка #7 (`ce3405b`) | `SUCCESS` — Полный пайплайн: сборка riot-server:7 (версия v2.0), автоматический rollout в k3s |

---

## 5. Ответы на контрольные вопросы методички

1. **Что такое Continuous Integration?**  
   Continuous Integration (непрерывная интеграция, CI) — практика разработки программного обеспечения, при которой разработчики регулярно (несколько раз в день) объединяют изменения исходного кода в общем репозитории Git, после чего каждое изменение автоматически собирается и тестируется для раннего выявления интеграционных ошибок.

2. **Что такое Continuous Delivery?**  
   Continuous Delivery (непрерывная доставка, CD) — методология, расширяющая CI, при которой каждое успешно протестированное изменение автоматически собирается в готовый к релизу артефакт (например, Docker-образ, NuGet-пакет) и может быть развернуто в тестовое или промышленное окружение нажатием одной кнопки (с ручным подтверждением).

3. **Чем Continuous Delivery отличается от Continuous Deployment?**  
   В Continuous Delivery шаг развертывания в продуктивную среду требует ручного подтверждения (ручной триггер / approval gate), тогда как в Continuous Deployment развертывание в production полностью автоматизировано: каждое изменение, прошедшее все этапы конвейера, немедленно раскатывается для конечных пользователей без участия человека.

4. **Что такое Pipeline?**  
   Pipeline (конвейер) — автоматизированная последовательность взаимосвязанных этапов (stages) и шагов (steps), описывающая весь жизненный цикл поставки ПО: от получения исходного кода из системы контроля версий до тестирования, сборки артефактов и их развертывания в целевой среде.

5. **Для чего используется Jenkins?**  
   Jenkins — это сервер автоматизации с открытым исходным кодом, предназначенный для оркестрации и выполнения задач CI/CD, управления пайплайнами, интеграции со сторонними инструментами (Git, Docker, Kubernetes, .NET SDK) и мониторинга процесса поставки ПО.

6. **Что такое Jenkins Job?**  
   Jenkins Job (задание) — сконфигурированная в Jenkins отдельная задача или проект сборки, определяющая, откуда брать код, какие триггеры запуска использовать, какие команды выполнять и как сохранять результаты работы.

7. **Чем Jenkins Pipeline отличается от обычного Jenkins Job?**  
   Обычный (Freestyle) Job настраивается вручную через веб-интерфейс, имеет линейную структуру и плохо подходит для сложного версионирования. Jenkins Pipeline реализует концепцию «Pipeline as Code», описывается в декларативном или скриптовом файле `Jenkinsfile`, хранится и версионируется вместе с кодом в Git и поддерживает сложные ветвления, параллельные шаги и визуализацию стадий.

8. **Что такое Jenkinsfile?**  
   Jenkinsfile — текстовый файл с декларативным или скриптовым кодом на базе синтаксиса Groovy/Jenkins DSL, хранящийся в корне репозитория проекта и содержащий полное определение структуры, стадий, шагов и условий выполнения CI/CD Pipeline.

9. **Для чего используется этап Restore?**  
   Этап `Restore` выполняет команду `dotnet restore`, загружая все объявленные в проектах внешние зависимости и NuGet-пакеты, проверяя их совместимость и подготавливая локальный кэш проекта к этапу компиляции.

10. **Почему тесты необходимо запускать до создания Docker-образа?**  
    Запуск тестов до создания образа предотвращает нецелевой расход ресурсов (времени сборки, сетевого трафика, места в Docker Registry) на упаковку заведомо неработоспособного кода. Кроме того, это гарантирует, что дефектные сборки не попадут в реестр образов и не будут по ошибке развернуты в кластере.

11. **Что происходит, если тесты завершаются ошибкой?**  
    Команда `dotnet test` возвращает ненулевой код завершения (exit code 1). Jenkins регистрирует сбой на этапе `Test`, маркирует статус сборки как `FAILURE` и немедленно прерывает выполнение пайплайна. Последующие этапы (`Package` и `Deploy`) блокируются и пропускаются.

12. **Что выполняется на этапе Package?**  
    На этапе `Package` выполняется сборка исполняемого артефакта или контейнерного образа (`docker build -t riot-server:%BUILD_NUMBER% .`), в который упаковываются скомпилированные бинарные файлы приложения, среда выполнения .NET 8 Runtime и необходимые системные конфигурации.

13. **Для чего используется этап Deploy?**  
    Этап `Deploy` автоматизирует доставку собранного Docker-образа в целевую среду выполнения (кластер Kubernetes), обновляет манифесты развертывания (`kubectl set image`), инициирует плавное обновление подов (Rolling Update) и проверяет доступность обновленного сервиса.

14. **Как Jenkins может обновить Docker-образ в Kubernetes Deployment?**  
    Jenkins запускает команду `kubectl set image deployment/<имя_deployment> <имя_контейнера>=<новый_образ>:<тег>` (либо применяет обновленный YAML-манифест через `kubectl apply`), обращаясь к Kubernetes API через настроенный файл `kubeconfig` или сервисный аккаунт (`ServiceAccount`).

15. **Для чего используется команда kubectl rollout status?**  
    Команда `kubectl rollout status deployment/<имя>` отслеживает статус выполнения обновления в режиме реального времени и завершается только тогда, когда все новые реплики Pod перешли в состояние `Running` и `Ready`, а старые реплики были корректно завершены. Если развертывание зависает (например, из-за ошибки вытягивания образа), команда завершается по таймауту с ошибкой, позволяя Jenkins зафиксировать сбой.

16. **Почему Jenkins должен иметь доступ к Kubernetes?**  
    Jenkins выполняет оркестрацию процесса развертывания. Без доступа к API-серверу Kubernetes (через учетные данные `kubeconfig` или токен кластера) Jenkins не может передавать команды создания ресурсов, обновления образов контейнеров и контроля статуса раскатки.

17. **Что такое Poll SCM?**  
    Poll SCM — встроенный триггер Jenkins, периодически опрашивающий систему контроля версий (Git) по заданному расписанию (cron-выражению) на предмет появления новых коммитов в отслеживаемой ветке. При обнаружении изменений Jenkins автоматически инициирует выполнение Pipeline.

18. **Что может привести к неуспешному выполнению Jenkins Pipeline?**  
    - Ошибки авторизации или недоступность Git-репозитория на этапе `Checkout`.
    - Несовместимость зависимостей или отсутствие доступа к NuGet-репозиторию на этапе `Restore`.
    - Ошибки компиляции исходного кода (синтаксические ошибки, нарушение типов) на этапе `Build`.
    - Провал хотя бы одного юнит- или интеграционного теста на этапе `Test`.
    - Ошибки в `Dockerfile` или падение Docker daemon на этапе `Package`.
    - Сетевая недоступность Kubernetes API, отсутствие прав, неверное имя Deployment или ошибка вытягивания образа (`ImagePullBackOff`) на этапе `Deploy`.
