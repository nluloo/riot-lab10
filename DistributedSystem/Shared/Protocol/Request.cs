using System.Text.Json;

namespace Shared.Protocol;

public class Request
{
    public string Operation { get; set; } = string.Empty;
    public JsonElement Data { get; set; }
}
