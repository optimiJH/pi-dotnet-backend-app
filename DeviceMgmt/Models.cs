// DeviceMgmt/Models.cs
using System.Collections.Concurrent;
using System.Net.WebSockets;

namespace SensorWsServer.DeviceMgmt;

public enum DeviceStatus { Online, Stale, Offline, Blocked }

public record DeviceDto(Guid Id, string Name, DateTimeOffset RegisteredAt, DateTimeOffset? LastSeenAt, DeviceStatus Status);

public class Device
{
    public Guid Id { get; init; } = Guid.NewGuid();
    public string Name { get; set; } = "";
    public DateTimeOffset RegisteredAt { get; init; } = DateTimeOffset.UtcNow;
    public DateTimeOffset? LastSeenAt { get; set; }
    public DeviceStatus Status { get; set; } = DeviceStatus.Offline;
    public string Token { get; set; } = ""; // plain for v1; hash later
}

public class DeviceCommand
{
    public Guid Id { get; init; } = Guid.NewGuid();
    public Guid DeviceId { get; init; }
    public string Type { get; init; } = "";        // reboot|update|run_script
    public string PayloadJson { get; init; } = "{}";
    public string Status { get; set; } = "queued";  // queued|sent|acked|failed
    public DateTimeOffset CreatedAt { get; init; } = DateTimeOffset.UtcNow;
    public DateTimeOffset? AckAt { get; set; }
    public string? Result { get; set; }
}

public record EnrollRequest(string DeviceFingerprint, string EnrollmentKey);
public record CommandRequest(string Type, System.Text.Json.JsonElement Payload);

// Shared in-memory store
public static class InMemStore
{
    public static readonly ConcurrentDictionary<Guid, Device> Devices = new();
    public static readonly ConcurrentDictionary<Guid, DeviceCommand> Commands = new(); // <-- DeviceCommand
    public static readonly ConcurrentDictionary<Guid, WebSocket> LiveSockets = new();
    // One-time enrollment invites (key -> expiry)
    public static readonly ConcurrentDictionary<string, DateTimeOffset> Invites = new();

    static InMemStore()
    {
        Persistence.Load();
    }

}

// DTO the client/front-end will receive
public record InviteDto(string EnrollmentKey, DateTimeOffset ExpiresAt);
