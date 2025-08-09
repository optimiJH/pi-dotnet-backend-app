using Microsoft.AspNetCore.Mvc;
using System.Text.Json;
using SensorWsServer.DeviceMgmt; 

[ApiController]
public class DevicesController : ControllerBase
{
    [HttpPost("api/devices/enroll")]
    public IActionResult Enroll([FromBody] EnrollRequest req)
    {
        if (string.IsNullOrWhiteSpace(req.DeviceFingerprint) || string.IsNullOrWhiteSpace(req.EnrollmentKey))
            return BadRequest();

        // For v1: accept any key; create-or-get by fingerprint
        var existing = InMemStore.Devices.Values.FirstOrDefault(d => d.Name == req.DeviceFingerprint);
        var device = existing ?? new Device { Name = req.DeviceFingerprint, Status = DeviceStatus.Online, LastSeenAt = DateTimeOffset.UtcNow };
        device.Token = Guid.NewGuid().ToString("n");
        InMemStore.Devices[device.Id] = device;

        return Ok(new { deviceId = device.Id, token = device.Token });
    }

    [HttpGet("api/devices")]
    public ActionResult<IEnumerable<DeviceDto>> GetDevices()
    {
        var now = DateTimeOffset.UtcNow;
        IEnumerable<DeviceDto> list = InMemStore.Devices.Values.Select(d =>
        {
            var last = d.LastSeenAt;
            var status = d.Status;
            if (d.Status != DeviceStatus.Blocked)
            {
                if (last is not null)
                {
                    var age = now - last.Value;
                    status = age < TimeSpan.FromMinutes(3) ? DeviceStatus.Online
                           : age < TimeSpan.FromMinutes(10) ? DeviceStatus.Stale
                           : DeviceStatus.Offline;
                }
            }
            return new DeviceDto(d.Id, d.Name, d.RegisteredAt, d.LastSeenAt, status);
        });
        return Ok(list);
    }

    [HttpPost("api/devices/{id:guid}/commands")]
    public IActionResult PostCommand(Guid id, [FromBody] CommandRequest req)
    {
        if (!InMemStore.Devices.ContainsKey(id)) return NotFound();
        var cmd =  new DeviceCommand{ DeviceId = id, Type = req.Type, PayloadJson = req.Payload.GetRawText(), Status = "queued" };
        InMemStore.Commands[cmd.Id] = cmd;

        // If WS connected, push immediately
        if (InMemStore.LiveSockets.TryGetValue(id, out var ws))
        {
            var payload = JsonSerializer.SerializeToUtf8Bytes(new {
                type = "command",
                commandId = cmd.Id,
                name = cmd.Type,
                payload = JsonSerializer.Deserialize<object>(cmd.PayloadJson)
            });
            if (ws.State == System.Net.WebSockets.WebSocketState.Open)
            {
                ws.SendAsync(new ArraySegment<byte>(payload), System.Net.WebSockets.WebSocketMessageType.Text, true, HttpContext.RequestAborted).GetAwaiter().GetResult();
                cmd.Status = "sent";
            }
        }
        return Ok(new { commandId = cmd.Id, status = cmd.Status });
    }
}

public record EnrollRequest(string DeviceFingerprint, string EnrollmentKey);
public record CommandRequest(string Type, JsonElement Payload);
