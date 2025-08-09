// Program.cs  (.NET 8 minimal API)

using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using SensorWsServer.DeviceMgmt; // uses InMemStore + models from DeviceMgmt/Models.cs

var builder = WebApplication.CreateBuilder(args);

/* ---------- ①  CORS for Angular ---------- */
builder.Services.AddCors(options =>
{
    options.AddPolicy("AllowAngular",
        p => p.WithOrigins("http://localhost:4200")
              .AllowAnyHeader()
              .AllowAnyMethod());
});

var app = builder.Build();

/* ---------- ②  enable CORS ---------- */
app.UseCors("AllowAngular");

/* ========================================================================== */
/*  A) ORIGINAL SENSOR PIPELINE (unchanged)                                   */
/* ========================================================================== */

/* 1) Shared, thread-safe store of all readings */
var readings = new ConcurrentQueue<SensorReading>();

/* 2) Plain HTTP probe  → GET / */
app.MapGet("/", () => "Sensor WebSocket server is running.");

/* 3) Data dump endpoint → GET /readings */
app.MapGet("/readings", () => Results.Json(readings.ToArray()));

/* 4) WebSocket endpoint → /ws  (existing sensor readings) */
app.UseWebSockets(new WebSocketOptions { KeepAliveInterval = TimeSpan.FromSeconds(20) });

app.Map("/ws", async (HttpContext ctx) =>
{
    if (!ctx.WebSockets.IsWebSocketRequest)
    {
        ctx.Response.StatusCode = StatusCodes.Status400BadRequest;
        return;
    }

    using var socket = await ctx.WebSockets.AcceptWebSocketAsync();
    var buffer = new byte[8192];

    var ms = new MemoryStream();
    while (true)
    {
        var result = await socket.ReceiveAsync(buffer, CancellationToken.None);

        if (result.MessageType == WebSocketMessageType.Close)
        {
            await socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Client closed", CancellationToken.None);
            break;
        }

        ms.Write(buffer, 0, result.Count);

        if (result.EndOfMessage)
        {
            var json = Encoding.UTF8.GetString(ms.ToArray());
            ms.SetLength(0);

            try
            {
                var r = JsonSerializer.Deserialize<SensorReading>(json,
                        new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

                if (r is not null)
                {
                    readings.Enqueue(r);
                    Console.WriteLine($"[{r.DeviceId}] T={r.Temperature:0.0}°C "
                                    + (r.Humidity is null ? "" : $"H={r.Humidity:0.0}% ")
                                    + (r.Pressure is null ? "" : $"P={r.Pressure:0.0}hPa "));
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Bad JSON: {ex.Message}\n{json}");
            }
        }
    }
});

/* ========================================================================== */
/*  B) NEW: DEVICE MANAGEMENT (enroll, list, command, device WebSocket)       */
/* ========================================================================== */

/* 5) Enroll → POST /api/devices/enroll
      Body: { "deviceFingerprint":"<hostname>", "enrollmentKey":"dev" } */
app.MapPost("/api/devices/enroll", (EnrollRequest req) =>
{
    if (string.IsNullOrWhiteSpace(req.DeviceFingerprint) || string.IsNullOrWhiteSpace(req.EnrollmentKey))
        return Results.BadRequest();

    // v1: accept any non-empty enrollmentKey
    var existing = InMemStore.Devices.Values.FirstOrDefault(d => d.Name == req.DeviceFingerprint);
    var device = existing ?? new Device { Name = req.DeviceFingerprint, Status = DeviceStatus.Online, LastSeenAt = DateTimeOffset.UtcNow };

    // issue a simple token (plain for v1)
    device.Token = Guid.NewGuid().ToString("n");
    InMemStore.Devices[device.Id] = device;

    return Results.Ok(new { deviceId = device.Id, token = device.Token });
});

/* 6) List devices → GET /api/devices */
app.MapGet("/api/devices", () =>
{
    var now = DateTimeOffset.UtcNow;
    var list = InMemStore.Devices.Values.Select(d =>
    {
        var status = d.Status;
        if (d.Status != DeviceStatus.Blocked && d.LastSeenAt is DateTimeOffset last)
        {
            var age = now - last;
            status = age < TimeSpan.FromMinutes(3) ? DeviceStatus.Online
                  : age < TimeSpan.FromMinutes(10) ? DeviceStatus.Stale
                  : DeviceStatus.Offline;
        }
        return new DeviceDto(d.Id, d.Name, d.RegisteredAt, d.LastSeenAt, status);
    });
    return Results.Json(list);
});

/* 7) Queue command → POST /api/devices/{id}/commands
      Body: { "type":"reboot|update|run_script", "payload":{...} } */
app.MapPost("/api/devices/{id:guid}/commands", async (Guid id, CommandRequest req, HttpContext http) =>
{
    if (!InMemStore.Devices.ContainsKey(id)) return Results.NotFound();

    var payloadJson = req.Payload.ValueKind == JsonValueKind.Undefined ? "{}" : req.Payload.GetRawText();

    var cmd = new DeviceCommand
    {
        DeviceId = id,
        Type = req.Type,
        PayloadJson = payloadJson,
        Status = "queued"
    };
    InMemStore.Commands[cmd.Id] = cmd;

    // If connected, push immediately
    if (InMemStore.LiveSockets.TryGetValue(id, out var ws) && ws.State == WebSocketState.Open)
    {
        var pushObj = new
        {
            type = "command",
            commandId = cmd.Id,
            name = cmd.Type,
            payload = JsonSerializer.Deserialize<object>(cmd.PayloadJson)
        };
        var bytes = JsonSerializer.SerializeToUtf8Bytes(pushObj);
        await ws.SendAsync(bytes, WebSocketMessageType.Text, true, http.RequestAborted);
        cmd.Status = "sent";
    }

    return Results.Ok(new { commandId = cmd.Id, status = cmd.Status });
});

/* 8) Device WebSocket (management channel) → /device-ws
      Query: ?deviceId=<guid>&token=<string> */
app.Map("/device-ws", async (HttpContext ctx) =>
{
    if (!ctx.WebSockets.IsWebSocketRequest)
    {
        ctx.Response.StatusCode = StatusCodes.Status400BadRequest;
        return;
    }

    var deviceIdQ = ctx.Request.Query["deviceId"].FirstOrDefault();
    var tokenQ = ctx.Request.Query["token"].FirstOrDefault();

    if (!Guid.TryParse(deviceIdQ, out var deviceId) || string.IsNullOrWhiteSpace(tokenQ))
    {
        ctx.Response.StatusCode = StatusCodes.Status401Unauthorized;
        return;
    }

    if (!InMemStore.Devices.TryGetValue(deviceId, out var device) || device.Token != tokenQ || device.Status == DeviceStatus.Blocked)
    {
        ctx.Response.StatusCode = StatusCodes.Status401Unauthorized;
        return;
    }

    using var ws = await ctx.WebSockets.AcceptWebSocketAsync();
    InMemStore.LiveSockets[deviceId] = ws;

    device.LastSeenAt = DateTimeOffset.UtcNow;

    var buffer = new byte[64 * 1024];
    try
    {
        while (ws.State == WebSocketState.Open)
        {
            var result = await ws.ReceiveAsync(new ArraySegment<byte>(buffer), ctx.RequestAborted);
            if (result.MessageType == WebSocketMessageType.Close) break;

            var msg = JsonDocument.Parse(Encoding.UTF8.GetString(buffer, 0, result.Count)).RootElement;
            var type = msg.GetProperty("type").GetString();

            if (type == "heartbeat")
            {
                device.LastSeenAt = DateTimeOffset.UtcNow;
                device.Status = DeviceStatus.Online;
            }
            else if (type == "commandAck")
            {
                var cid = msg.GetProperty("commandId").GetGuid();
                if (InMemStore.Commands.TryGetValue(cid, out var c))
                {
                    c.Status = msg.TryGetProperty("status", out var s) ? (s.GetString() ?? "acked") : "acked";
                    c.AckAt = DateTimeOffset.UtcNow;
                    c.Result = msg.TryGetProperty("result", out var r) ? r.GetString() : null;
                }
            }
        }
    }
    finally
    {
        InMemStore.LiveSockets.TryRemove(deviceId, out _);
    }
});

/* 9) Listen on all interfaces, port 5005 */
app.Run("http://0.0.0.0:5005");

/* Sensor DTO (existing) */
public record SensorReading(
    string DeviceId,
    double Temperature,
    double? Humidity,
    double? Pressure,
    long Timestamp
);
