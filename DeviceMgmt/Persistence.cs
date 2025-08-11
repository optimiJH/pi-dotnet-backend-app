// Persistence.cs
using System.Text.Json;

namespace SensorWsServer.DeviceMgmt
{
    internal static class Persistence
    {
        private static readonly string DataDir      = Path.Combine(AppContext.BaseDirectory, "data");
        private static readonly string DevicesFile  = Path.Combine(DataDir, "devices.json");
        private static readonly string CommandsFile = Path.Combine(DataDir, "commands.json");

        private static readonly JsonSerializerOptions JsonOpts = new(JsonSerializerDefaults.Web)
        {
            WriteIndented = true
        };

        public static void Load()
        {
            Directory.CreateDirectory(DataDir);

            if (File.Exists(DevicesFile))
            {
                var list = JsonSerializer.Deserialize<List<Device>>(File.ReadAllText(DevicesFile), JsonOpts) ?? new();
                foreach (var d in list) InMemStore.Devices[d.Id] = d;
            }

            if (File.Exists(CommandsFile))
            {
                var list = JsonSerializer.Deserialize<List<DeviceCommand>>(File.ReadAllText(CommandsFile), JsonOpts) ?? new();
                foreach (var c in list) InMemStore.Commands[c.Id] = c;
            }

            Console.WriteLine($"[persist] loaded {InMemStore.Devices.Count} devices, {InMemStore.Commands.Count} commands");
        }

        public static void SaveDevices()
        {
            Directory.CreateDirectory(DataDir);
            var snapshot = InMemStore.Devices.Values.OrderBy(d => d.RegisteredAt).ToList();
            File.WriteAllText(DevicesFile, JsonSerializer.Serialize(snapshot, JsonOpts));
        }

        public static void SaveCommands()
        {
            Directory.CreateDirectory(DataDir);
            var snapshot = InMemStore.Commands.Values.ToList();
            File.WriteAllText(CommandsFile, JsonSerializer.Serialize(snapshot, JsonOpts));
        }
    }
}
