using System;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;

class Program
{
    // Use static fields so they can be referenced from Main()
    private static string apiUrl = "http://192.168.1.55:8000/consumer";
    private static string topic = "example_agent_a_kel2zq";
    private static string privateKeyPath = @"C:\Users\abarrio\OneDrive - JPL\Desktop\Digital Twin Interoperability\Codes\HSML codes\testing_lab_verification\registeredExamplesLabDB\private_key_example_agent_B.pem";

    static async Task Main(string[] args)
    {
        using var client = new HttpClient();

        // Step 1: Read the private key file
        byte[] privateKeyBytes;
        try
        {
            privateKeyBytes = File.ReadAllBytes(privateKeyPath);
        }
        catch (Exception e)
        {
            Console.WriteLine("Failed to read private key: " + e.Message);
            return;
        }

        // Step 2: Authorize using /authorize endpoint.
        // Create a multipart form-data request containing only the private key.
        var authContent = new MultipartFormDataContent();
        authContent.Add(new ByteArrayContent(privateKeyBytes), "private_key", "private_key.pem");
        // Pass the topic as a query parameter rather than in the form data.
        string authUrl = $"{apiUrl}/authorize?topic={Uri.EscapeDataString(topic)}";

        Console.WriteLine("Authorizing...");
        var authResponse = await client.PostAsync(authUrl, authContent);
        string authResponseBody = await authResponse.Content.ReadAsStringAsync();
        Console.WriteLine("Auth Response:");
        Console.WriteLine(authResponseBody);
        if (!authResponse.IsSuccessStatusCode)
            return;

        // Step 3: Start consumer using /start endpoint.
        // The API expects the topic as a query parameter.
        string startUrl = $"{apiUrl}/start?topic={Uri.EscapeDataString(topic)}";
        var startResponse = await client.PostAsync(startUrl, null);
        string startResponseBody = await startResponse.Content.ReadAsStringAsync();
        Console.WriteLine("Start Response:");
        Console.WriteLine(startResponseBody);
        if (!startResponse.IsSuccessStatusCode)
            return;

        // Wait for user input before stopping the consumer.
        Console.WriteLine("Consumer is running. Press ENTER to stop...");
        Console.ReadLine();

        // Step 4: Stop consumer using /stop endpoint.
        string stopUrl = $"{apiUrl}/stop?topic={Uri.EscapeDataString(topic)}";
        var stopResponse = await client.PostAsync(stopUrl, null);
        string stopResponseBody = await stopResponse.Content.ReadAsStringAsync();
        Console.WriteLine("Stop Response:");
        Console.WriteLine(stopResponseBody);
    }
}
