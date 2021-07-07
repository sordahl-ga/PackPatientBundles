using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using Newtonsoft.Json.Linq;
using System.Linq;
using Azure.Storage.Blobs;
using System.Threading;
using System.Text;

namespace PackPatientBundles
{
    class Program
    {

        static void Main(string[] args)
        {
            if (args.Length < 2)
            {
                Console.WriteLine($"PackPatientBundles: usage packpatientbundles <pack|count|upload|export|liveload> <source path> [destpath or connectionstring] [resource types]");
                return;
            }
            string cd = args[1];
            string dest = Directory.GetCurrentDirectory() + Path.PathSeparator + Guid.NewGuid().ToString().Replace("-","");
            if (args.Length > 2)
            {
                dest = args[2];
            }
            switch (args[0])
            {
                case "liveload":
                    if (args.Length < 4)
                    {
                        Console.WriteLine($"Must Provide a sourcedirectory, destination connection string and resources per minute to simulatelive loading");
                        break;
                    }
                    if (!args[2].StartsWith("DefaultEndpointsProtocol"))
                    {
                        Console.WriteLine($"{args[2]} is not a valid Azure Storage Connection String");
                        break;
                    }
                    if (int.TryParse(args[3], out int rate))
                        simulateliveload(cd, args[2], rate);
                    else
                        Console.WriteLine($"{args[3]} is not a valid integer for rate of resources sent per minute");
                    break;
                case "export":
                    exportFHIRIDs(cd);
                    break;
                case "count":
                    if (cd.EndsWith("zip") && File.Exists(cd))
                    {
                        countResources(cd);
                    }
                    else if (Directory.Exists(cd))
                    {
                        countDir(cd);
                    } else
                    {
                        Console.WriteLine($"{cd} is not valid file or directory");
                    }
                    break;
                case "pack":
                    string s_resources = "Patient";
                    if (args.Length > 3)
                    {
                        s_resources = args[3];
                    }
                    if (!File.Exists(cd) || !cd.EndsWith(".zip"))
                    {
                        Console.WriteLine($"PackPatientBundles: File {cd} does not exist or is not a zip file");
                        break;
                    }
                    string[] resources = s_resources.Split(",");
                    JObject rv = initBundle();
                    int total = 0;
                    int bundlecnt = 0;
                    int filecnt = 0;
                    int compfilecnt = 0;
                    int destzipentries = 0;
                    int zipfileno = 1;
                    Console.WriteLine($"Processing JSON files in {cd}...Packing these resources {s_resources}...");

                    using (ZipArchive archive = new ZipArchive(File.OpenRead(cd)))
                    {
                        FileStream zipToCreate = new FileStream($"{dest}-{zipfileno++}.zip", FileMode.Create);
                        ZipArchive destarch = new ZipArchive(zipToCreate, ZipArchiveMode.Update);
                        foreach (ZipArchiveEntry entry in archive.Entries)
                        {
                            filecnt++;
                            if (entry.Name.StartsWith("bundle")) continue;
                            using (var zipStream = entry.Open())
                            {
                                StreamReader sr = new StreamReader(zipStream);
                                string contents = sr.ReadToEnd();
                                JObject bundle = TransformBundle(contents);
                                JArray entries = (JArray)bundle["entry"];
                                foreach (JToken tok in entries)
                                {
                                    string rt = tok["resource"]["resourceType"].ToString();
                                    if (resources.Contains(rt)) { 
                                            addResource(rv, tok["resource"]);
                                            bundlecnt++;
                                    }
                                    if (bundlecnt >= 300)
                                    {
                                        string fn = $"bundle{Guid.NewGuid().ToString().Replace("-", "")}.json";
                                        ZipArchiveEntry fileentry = destarch.CreateEntry(fn);
                                        using (StreamWriter writer = new StreamWriter(fileentry.Open()))
                                        {
                                            writer.Write(rv.ToString());
                                        }
                                        destzipentries++;
                                        compfilecnt++;
                                        total += bundlecnt;
                                        UpdateCon(filecnt, total, compfilecnt);
                                        bundlecnt = 0;
                                        rv = null;
                                        rv = initBundle();
                                        if (destzipentries >= 200)
                                        {
                                            destarch.Dispose();
                                            zipToCreate = new FileStream($"{dest}-{zipfileno++}.zip", FileMode.Create);
                                            destarch = new ZipArchive(zipToCreate, ZipArchiveMode.Update);
                                            destzipentries = 0;
                                        }

                                    }

                                }
                            }
                        }

                        if (bundlecnt > 0)
                        {
                            string fn = $"bundle{Guid.NewGuid().ToString().Replace("-", "")}.json";
                            ZipArchiveEntry fileentry = destarch.CreateEntry(fn);
                            using (StreamWriter writer = new StreamWriter(fileentry.Open()))
                            {
                                writer.Write(rv.ToString());
                            }
                            compfilecnt++;
                            total += bundlecnt;
                            UpdateCon(filecnt, total, compfilecnt);
                        }
                        destarch.Dispose();
                      
                    }

                    Console.WriteLine($"Finshed processing {filecnt} files with export of {total} resources packedinto {compfilecnt} files in {zipfileno-1} zip files...");
                    break;
                case "upload":
                    if (args.Length < 3)
                    {
                        Console.WriteLine($"Must Provide a sourcedirectory and destination connection string to upload");
                        break;
                    }
                    if (!args[2].StartsWith("DefaultEndpointsProtocol"))
                    {
                        Console.WriteLine($"{args[2]} is not a valid Azure Storage Connection String");
                        break;
                    }
                    uploadtoblob(cd, args[2]);
                    break;
                default:
                    Console.WriteLine($"PackPatientBundles: usage packpatientbundles <pack|count|upload|export> <source path> [destpath or connectionstring] [resource types]");
                    break;
            }
        }
       
        public static void UpdateCon(int filecnt,int total,int compfilecnt)
        {
            Console.Write($"\rProcessed {filecnt} files with {total} resources packedinto {compfilecnt} files...");
        }
        public static void cleanup(string cd)
        {
            Console.WriteLine($"Cleaning error resource in directory {cd}...");
            if (!Directory.Exists(cd))
            {
                Console.WriteLine($"{cd} is not a directory");
                return;
            }
            int fixcnt = 0;
            int filecnt = 0;
            foreach (string name in Directory.EnumerateFiles(cd, "*.actionneeded"))
            {
                Console.WriteLine($"Fixing {name}...");
                JObject fixobj = initBundle();
                JObject errobj = JObject.Parse(File.ReadAllText(name));
                JArray errors = (JArray)errobj["errors"];
                foreach (JToken tok in errors)
                {
                    string rt = tok["resource"]["resource"]["resourceType"].ToString();
                    string id = tok["resource"]["resource"]["id"].ToString();
                    if (id.StartsWith($"{rt}/")) tok["resource"]["resource"]["id"] = id.Replace($"{rt}/", "");
                    addResource(fixobj, tok["resource"]["resource"]);
                    fixcnt++;
                }
                File.WriteAllText(name + ".fixed.json", fixobj.ToString());
                filecnt++;
            }
            Console.WriteLine($"Processed {filecnt} files, fixed {fixcnt} resources.");

        }
        public static void countDir(string cd)
        {
            int grand = 0;
            int filecnt = 0;
            if (Directory.Exists(cd))
            {
                foreach (var filename in Directory.EnumerateFiles(cd,"*.zip"))
                {
                    filecnt++;
                    grand += countResources(filename);
                }
            }
            Console.WriteLine($"Grand total of all resources contained in {filecnt} zip files is {grand}");
        }
        public static void exportFHIRIDs(string cd)
        {
            int grand = 0;

            if (Directory.Exists(cd))
            {
                using (StreamWriter writer = new StreamWriter(cd + "/fhirids.txt"))
                {
                    foreach (var filename in Directory.EnumerateFiles(cd, "*.ndjson"))
                    {
                        Console.Write($"\rProcessing file {filename}...");
                        using (StreamReader reader = new StreamReader(filename))
                        {
                            string line;
                            while ((line = reader.ReadLine()) != null)
                            {
                                var tok = JObject.Parse(line);
                                string id = (string)tok["id"];
                                if (id != null)
                                {
                                    writer.WriteLine(id);
                                    grand++;
                                }
                            }

                        }
                    }
                    Console.WriteLine("");
                    Console.WriteLine($"Completed processing files in {cd} total of {grand} resource ids");
                }
            }
            else
            {
                Console.WriteLine($"Directory {cd} is invalid");
            }
        }
        public static int countResources(string filename)
        {
            Console.WriteLine($"Counting resources contained in bundles {filename}...");
            int filecnt = 0;
            int total = 0;
            Dictionary<string, int> countByResourceType = new Dictionary<string, int>();
            using (ZipArchive archive = new ZipArchive(File.OpenRead(filename)))
            {
                foreach (ZipArchiveEntry entry in archive.Entries)
                {
                  
                   using (var zipStream = entry.Open())
                    {
                        StreamReader sr = new StreamReader(zipStream);
                        string contents = sr.ReadToEnd();
                        if (string.IsNullOrEmpty(contents))
                        {
                            Console.WriteLine($"{entry.Name} is empty or invalid.");
                            continue;
                        }
                        JObject bundle = JObject.Parse(contents);
                        JArray entries = (JArray)bundle["entry"];
                        foreach (JToken tok in entries)
                        {
                            string rt = tok["resource"]["resourceType"].ToString();
                            int restotal = 0;
                            countByResourceType.TryGetValue(rt, out restotal);
                            restotal++;
                            countByResourceType[rt] = restotal;
                            total++;
                        }
                        filecnt++;
                        Console.Write($"\rProcessed {filecnt} files containing {total} resources...");
                    }
                }
            }
            Console.WriteLine("");
            Console.WriteLine($"Resource Breakdown Overall Total {total}: ");
            Console.WriteLine("-----------------------------------------------");
            foreach(var key in countByResourceType)
            {
                Console.WriteLine($"{key.Key}: {key.Value}");
            }
            return total;

        }

        public static void splitzips(string filename, int maxfilesperzip = 300)
        {
            using (ZipArchive archive = new ZipArchive(File.OpenRead(filename)))
            {
                if (archive.Entries.Count > maxfilesperzip)
                {

                }
            }
        }
        private static void blobsinqueue(BlobContainerClient client, int holdthreshold = -1, int delayms=5000)
        {
            Console.Write("Checking outstanding blobs queue...");
            int outblobs = client.GetBlobs().Count();
            if (outblobs == 0) {
                Console.WriteLine("no outstanding blobs in queue.");
                return;
            }
            if (holdthreshold > -1)
            {
                while (outblobs > holdthreshold)
                {

                    Console.WriteLine($"Still {outblobs} to be processed...Waiting for {delayms} ms for queue to drain to {holdthreshold}...");
                    Thread.Sleep(delayms);
                    outblobs = client.GetBlobs().Count();
                }
                Console.WriteLine($"{outblobs} is below queue threshold....");
            } else
            {
                Console.WriteLine($"{outblobs} are in queue");
            }

        }
        public static void simulateliveload(string source,string connectstring,int resourcesperminute)
        {
            if (!Directory.Exists(source))
            {
                Console.WriteLine($"Path {source} does not exist...");
            }
            string filter = "*.zip";
            // Create a BlobServiceClient object which will be used to create a container client
            BlobServiceClient blobServiceClient = new BlobServiceClient(connectstring);
            //Bulk Loader Zip Container 
            string bundleName = "bundles";
            // Create the container and return a container client object
            BlobContainerClient bundleClient = blobServiceClient.GetBlobContainerClient(bundleName);
            DateTime start = DateTime.Now;
            Console.WriteLine($"Uploading from {filter} files in {source}...");
            int bundlecnt = 0;
            foreach (var filename in Directory.EnumerateFiles(source, filter))
            {
                using (ZipArchive archive = new ZipArchive(File.OpenRead(filename)))
                {
                    foreach (ZipArchiveEntry entry in archive.Entries)
                    {
                        using (var zipStream = entry.Open())
                        {
                            StreamReader sr = new StreamReader(zipStream);
                            string contents = sr.ReadToEnd();
                            var o = JObject.Parse(contents);
                            JArray arr = (JArray)o["entry"];
                            if (arr != null) bundlecnt += arr.Count;
                            Console.WriteLine($"Sending...{arr.Count} resources from {entry.Name}...");
                            byte[] byteArray = Encoding.UTF8.GetBytes(contents);
                            MemoryStream stream = new MemoryStream(byteArray);
                            BlobClient blobClient = bundleClient.GetBlobClient(entry.Name);
                            blobClient.Upload(stream, true);
                            stream.Close();
                            if (bundlecnt >= resourcesperminute)
                            {
                                int tc = 60;
                                while (tc > 0)
                                {
                                    Console.Write($"\rAllowing {tc.ToString("D2")} seconds between {resourcesperminute} resources...");
                                    Thread.Sleep(1000);
                                    tc--;
                                }
                                blobsinqueue(bundleClient, 50);
                                bundlecnt = 0;
                            }
                        }
                    }
                }
            }
            DateTime stop = DateTime.Now;
            TimeSpan ts = stop.Subtract(start);
            Console.WriteLine($"Completed uploading {filter} files to blob storage account in {ts.TotalMinutes} minutes.");
        }
        public static void uploadtoblob(string source,string connectstring)
        {
            if (!Directory.Exists(source))
            {
                Console.WriteLine($"Path {source} does not exist...");
            }
            string filter = "*.zip";
            // Create a BlobServiceClient object which will be used to create a container client
            BlobServiceClient blobServiceClient = new BlobServiceClient(connectstring);
            //Bulk Loader Zip Container 
            string containerName = "zip";
            string bundleName = "bundles";
            // Create the container and return a container client object
            BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containerName);
            BlobContainerClient bundleClient = blobServiceClient.GetBlobContainerClient(bundleName);
            DateTime start = DateTime.Now;
            Console.WriteLine($"Uploading {filter} files from {source}...");
                     
            foreach (var filename in Directory.EnumerateFiles(source, filter))
            {
                string bfn = Path.GetFileName(filename);
                Console.WriteLine($"Uploading...{bfn}");
                BlobClient blobClient = containerClient.GetBlobClient(bfn);
                using FileStream uploadFileStream = File.OpenRead(filename);
                blobClient.Upload(uploadFileStream, true);
                uploadFileStream.Close();
                Console.WriteLine($"Uploaded {bfn} to blob container {containerName}...");
                int tc = 15;
                while (tc > 0)
                {
                    Console.Write($"\rAllowing file expansion processing time....{tc.ToString("D2")} seconds");
                    Thread.Sleep(1000);
                    tc--;
                }
                blobsinqueue(bundleClient,50);
            }
            DateTime stop = DateTime.Now;
            TimeSpan ts = stop.Subtract(start);
            Console.WriteLine($"Completed uploading {filter} files to blob storage account in {ts.TotalMinutes} minutes.");

        }
        
        
        public static JObject TransformBundle(string contents)
        {
            JObject result = JObject.Parse(contents);
            if (result == null || result["resourceType"] == null || result["type"] == null) throw new Exception("Not a valid bundle json file");
            string rtt = result["resourceType"].ToString();
            string bt = (string)result["type"];
            if (rtt.Equals("Bundle") && bt.Equals("transaction"))
            {
                    //reparse JSON with replacement of existing ids prepare to convert to Batch bundle with PUT to maintain relationships
                    Dictionary<string, string> convert = new Dictionary<string, string>();
                    result["type"] = "batch";
                    JArray entries = (JArray)result["entry"];
                    foreach (JToken tok in entries)
                    {
                        string urn = (string)tok["fullUrl"];
                        if (!string.IsNullOrEmpty(urn) && tok["resource"]!=null)
                        {
                            string rt = (string)tok["resource"]["resourceType"];
                            string rid = (string)tok["resource"]["id"];
                            if (string.IsNullOrEmpty(rid))
                            {
                                rid = urn.Replace("urn:uuid:", "");
                                tok["resource"]["id"] = rid;
                            }
                            if (!convert.TryAdd(rid, rt))
                            {
                                Console.WriteLine($"**** Duplicate GUID Detected {rid} already assigned to a resource type");
                            }
                            tok["request"]["method"] = "PUT";
                            tok["request"]["url"] = $"{rt}?_id={rid}";
                        }

                    }
                //log.LogInformation($"TransformBundleProcess: Phase 2 Localizing {convert.Count} resource entries...");
                IEnumerable<JToken> refs = result.SelectTokens("$..reference");
                foreach (JToken item in refs)
                {
                    string s = item.ToString();
                    string t = "";
                    s = s.Replace("urn:uuid:", "");

                    if (convert.TryGetValue(s, out t))
                    {
                        item.Replace(t + "/" + s);
                    }
                }
                //log.LogInformation($"TransformBundleProcess: Complete.");
                return result;
            }
            return result;
        
    }
    public static JObject initBundle()
        {
            JObject rv = new JObject();
            rv["resourceType"] = "Bundle";
            rv["type"] = "batch";
            rv["entry"] = new JArray();
            return rv;
        }
        public static void addResource(JObject bundle, JToken tok)
        {
            JObject rv = new JObject();
            string rt = (string)tok["resourceType"];
            string rid = (string)tok["id"];
            rv["fullUrl"] = $"{rt}/{rid}";
            rv["resource"] = tok;
            JObject req = new JObject();
            req["method"] = "PUT";
            req["url"] = $"{rt}?_id={rid}";
            rv["request"] = req;
            JArray entries = (JArray)bundle["entry"];
            entries.Add(rv);
        }
        public static List<IncludedResource> ResetIncludedCounts(List<IncludedResource> incr)
        {
            foreach(IncludedResource i in incr)
            {
                i.Sampled = 0;
            }
            return incr;
        }
    }
    public class IncludedResource
    {
        public string Resource { get; set; }
        public int SampleSize { get; set; }
        public int Sampled { get; set; }
        public bool Increment(int size = 1)
        {
            if (this.SampleSize < 0 || (this.Sampled + size <= this.SampleSize)) {
                this.Sampled++;
                return true;
            }
            return false;
        }
    }
}
