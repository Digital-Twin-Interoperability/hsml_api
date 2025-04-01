using System;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

class Program
{
    // API endpoint
    private static readonly string API_URL = "http://192.168.1.55:8000/producer";

    static async Task Main(string[] args)
    {
        string privateKeyPath = @"C:\Users\abarrio\OneDrive - JPL\Desktop\Digital Twin Interoperability\Codes\HSML codes\testing_lab_verification\registeredExamplesLabDB\private_key_example_agent_A.pem";
        string topic = "example_agent_a_kel2zq";
        string jsonFilePath = @"C:\Users\abarrio\OneDrive - JPL\Desktop\Digital Twin Interoperability\Codes\HSML codes\testing_lab_verification\registeredExamplesLabDB\Example_Agent_A.json";

        try
        {
            string fileContent = File.ReadAllText(jsonFilePath);
            Console.WriteLine("Read JSON file successfully.");

            // Optionally override privateKeyPath and topic if present in the JSON file.
            using (JsonDocument fileDoc = JsonDocument.Parse(fileContent))
            {
                JsonElement root = fileDoc.RootElement;
                if (root.TryGetProperty("private_key_path", out JsonElement pkElem))
                    privateKeyPath = pkElem.GetString();
                if (root.TryGetProperty("topic", out JsonElement topicElem))
                    topic = topicElem.GetString();
            }

            using (HttpClient client = new HttpClient())
            {
                // -------------------- /authenticate request (multipart/form-data) --------------------
                var multipartContent = new MultipartFormDataContent();
                byte[] fileBytes = File.ReadAllBytes(privateKeyPath);
                var fileContentForUpload = new ByteArrayContent(fileBytes);
                fileContentForUpload.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
                // The API expects the file field name to be "private_key"
                multipartContent.Add(fileContentForUpload, "private_key", Path.GetFileName(privateKeyPath));
                
                // The API expects the topic as a query parameter for authentication.
                string authUrl = $"{API_URL}/authenticate?topic={Uri.EscapeDataString(topic)}";
                HttpResponseMessage authResponse = await client.PostAsync(authUrl, multipartContent);
                string authResponseStr = await authResponse.Content.ReadAsStringAsync();

                Console.WriteLine("Auth Response:");
                PrintFormattedJson(authResponseStr);

                // -------------------- /start request (JSON body with query parameter) --------------------
                var startPayload = new
                {
                    json_message = JsonDocument.Parse(fileContent).RootElement
                };

                string startJson = JsonSerializer.Serialize(startPayload);
                var startContent = new StringContent(startJson, Encoding.UTF8, "application/json");
                string startUrl = $"{API_URL}/start?topic={Uri.EscapeDataString(topic)}";
                HttpResponseMessage startResponse = await client.PostAsync(startUrl, startContent);
                string startResponseStr = await startResponse.Content.ReadAsStringAsync();

                Console.WriteLine("Start Response:");
                PrintFormattedJson(startResponseStr);

                // -------------------- Wait and then send /stop request --------------------
                Console.WriteLine("Press any key to send stop request...");
                Console.ReadKey();

                // Call /stop endpoint. The API expects topic as a query parameter.
                string stopUrl = $"{API_URL}/stop?topic={Uri.EscapeDataString(topic)}";
                HttpResponseMessage stopResponse = await client.PostAsync(stopUrl, null);
                string stopResponseStr = await stopResponse.Content.ReadAsStringAsync();

                Console.WriteLine("Stop Response:");
                PrintFormattedJson(stopResponseStr);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error: {ex.Message}");
        }
    }

    static void PrintFormattedJson(string rawJson)
    {
        try
        {
            using JsonDocument doc = JsonDocument.Parse(rawJson);
            string pretty = JsonSerializer.Serialize(doc.RootElement, new JsonSerializerOptions { WriteIndented = true });
            Console.WriteLine(pretty);
        }
        catch
        {
            Console.WriteLine(rawJson);
        }
    }
}
