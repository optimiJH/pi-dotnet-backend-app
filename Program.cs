// Program.cs  (.NET 8 minimal API)

using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;

var builder = WebApplication.CreateBuilder(args);


/* ---------- ①  register a CORS policy ---------- */
builder.Services.AddCors(options =>
{
    options.AddPolicy("AllowAngular",
        p => p.WithOrigins("http://localhost:4200")   // <-- Angular dev server
              .AllowAnyHeader()
              .AllowAnyMethod());
});

var app = builder.Build();

/* ---------- ②  enable the policy before other middlewares ---------- */
app.UseCors("AllowAngular");
// var app     = builder.Build();

/* ---------------------------------------------------------------------------
 * 1.  Shared, thread-safe store of all readings
 * --------------------------------------------------------------------------- */
var readings = new ConcurrentQueue<SensorReading>();

/* ---------------------------------------------------------------------------
 * 2.  Plain HTTP probe  → GET /
 * --------------------------------------------------------------------------- */
app.MapGet("/", () => "Sensor WebSocket server is running.");

/* ---------------------------------------------------------------------------
 * 3.  Data dump endpoint → GET /readings
 * --------------------------------------------------------------------------- */
app.MapGet("/readings", () => Results.Json(readings.ToArray()));

/* ---------------------------------------------------------------------------
 * 4.  WebSocket endpoint → /ws  (unchanged auth logic optional)
 * --------------------------------------------------------------------------- */
app.UseWebSockets(new WebSocketOptions
{
    KeepAliveInterval = TimeSpan.FromSeconds(20)
});

app.Map("/ws", async (HttpContext ctx) =>
{
    if (!ctx.WebSockets.IsWebSocketRequest)
    {
        ctx.Response.StatusCode = StatusCodes.Status400BadRequest;
        return;
    }

    using var socket = await ctx.WebSockets.AcceptWebSocketAsync();
    var buffer       = new byte[8192];

    var ms = new MemoryStream();
    while (true)
    {
        var result = await socket.ReceiveAsync(buffer, CancellationToken.None);

        if (result.MessageType == WebSocketMessageType.Close)
        {
            await socket.CloseAsync(WebSocketCloseStatus.NormalClosure,
                                    "Client closed", CancellationToken.None);
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

/* ---------------------------------------------------------------------------
 * 5.  Listen on all interfaces, port 5005
 * --------------------------------------------------------------------------- */
app.Run("http://0.0.0.0:5005");

/* ---------------------------------------------------------------------------
 * DTO that matches payload coming from the Pi / fake client
 * --------------------------------------------------------------------------- */
public record SensorReading(
    string DeviceId,
    double Temperature,
    double? Humidity,
    double? Pressure,
    long   Timestamp
);
