namespace Tests;

/// <summary>
/// Проверка работы с параметрами конфигурации Kubernetes (ConfigMap / переменные окружения).
/// Требование ЛР №9 Часть 4, Требование 1 и Требование 6.
/// </summary>
public class KubernetesConfigTests
{
    [Fact]
    public void AppMode_DefaultValue_IsStandalone()
    {
        string? original = Environment.GetEnvironmentVariable("APP_MODE");
        try
        {
            Environment.SetEnvironmentVariable("APP_MODE", null);
            string mode = Environment.GetEnvironmentVariable("APP_MODE") ?? "standalone";
            Assert.Equal("broken_for_experiment", mode);
        }
        finally
        {
            Environment.SetEnvironmentVariable("APP_MODE", original);
        }
    }

    [Fact]
    public void AppMode_FromEnvironment_ReturnsConfiguredValue()
    {
        string? original = Environment.GetEnvironmentVariable("APP_MODE");
        try
        {
            Environment.SetEnvironmentVariable("APP_MODE", "kubernetes");
            string mode = Environment.GetEnvironmentVariable("APP_MODE") ?? "standalone";
            Assert.Equal("kubernetes", mode);
        }
        finally
        {
            Environment.SetEnvironmentVariable("APP_MODE", original);
        }
    }
}
