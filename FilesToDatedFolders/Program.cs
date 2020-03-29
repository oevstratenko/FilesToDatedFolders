using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace FilesToDatedFolders
{
    class Program
    {
        static void Main(string[] args)
        {
            string param = null;

            while (!new FilesToDatedFolders().FilesToDatedFoldersByParam(param))
            {
                param = Console.ReadLine().ToLower();
            }

            Console.ReadKey();
        }
    }

    public class FilesToDatedFolders
    {
        public static string[] validParam = Enum.GetValues(typeof(e_validParam)).Cast<e_validParam>().Select(x => x.ToString()).ToArray();
        enum e_validParam
        {
            season,
            year,
            month,
            day
        }

        readonly string rootFolder = ConfigurationManager.AppSettings["rootFolder"];
        readonly string filesFormat = ConfigurationManager.AppSettings["filesFormat"];
        string filesDb => $"{rootFolder}.txt";

        public bool FilesToDatedFoldersByParam(string aParam)
        {
            e_validParam param;
            if (!Enum.TryParse(aParam, out param))
            {
                Console.WriteLine("Set param for separate: {0}", string.Join(", ", validParam));
                return false;
            }

            switch (param)
            {
                case e_validParam.day:
                    FilesToDatedFoldersByFormat("yyyy-MM-dd");
                    break;
                case e_validParam.month:
                    FilesToDatedFoldersByFormat("yyyy-MM");
                    break;
                case e_validParam.year:
                    FilesToDatedFoldersByFormat("yyyy");
                    break;
                case e_validParam.season:
                    FilesToDatedFoldersByFormat(param.ToString());
                    break;
            }

            return true;
        }

        private string formatDate = null;

        public void FilesToDatedFoldersByFormat(string aFormatDate)
        {
            formatDate = aFormatDate;

            var startTime = DateTime.Now;

            var processedFilesSource = string.Empty;
            if (File.Exists(filesDb))
            {
                processedFilesSource += File.ReadAllText(filesDb);
            }
            var processedFiles = processedFilesSource.Split(';');

            string[] allfiles = //Directory.GetFiles(Directory.GetCurrentDirectory(), "*.*", SearchOption.AllDirectories);
            filesFormat.Split('|')
                .SelectMany(filter => Directory.GetFiles(Directory.GetCurrentDirectory(), filter, SearchOption.AllDirectories))
                .Where(fileName => !processedFiles.Contains(fileName) && !Path.GetDirectoryName(fileName).Contains("\\" + rootFolder + "\\"))
                .OrderBy(x => new FileInfo(x).LastWriteTime)
                .ToArray();

            if (!allfiles.Any())
            {
                Console.WriteLine("Found nothing");
                return;
            }

            Directory.CreateDirectory(rootFolder);

            using (StreamWriter txt = new StreamWriter(filesDb, append: true))
            {
                foreach (var file in allfiles)
                {
                    addFileToTaskPool(file, txt);
                }

                Task.WaitAll(taskPool.ToArray());//Ожидание завершения процессов
            }

            var durationTime = DateTime.Now - startTime;

            Console.WriteLine("Processed {0} files ({3} Mb TOTAL) in {1} min {2} sec", cnt, Math.Round(durationTime.TotalMinutes), durationTime.Seconds, Math.Round(ConvertBytesToMegabytes(size), 2));
        }

        private long size = 0;
        private int cnt = 0;
        private static object locker = new object();

        private void copyFile(string aFile, StreamWriter aWriter)
        {
            FileInfo f = new FileInfo(aFile);

            string toFolder = formatDate.ToLower() == e_validParam.season.ToString() ? $"{f.LastWriteTime:yyyy}-{GetSeason(f.LastWriteTime)}" : f.LastWriteTime.ToString(formatDate);
            var newFileDirectory = $"{Directory.GetCurrentDirectory()}\\{rootFolder}\\{toFolder}";

            Directory.CreateDirectory(newFileDirectory);

            int _cnt;
            long _size;

            lock (locker)
            {
                _cnt = cnt += 1;
                _size = size += (int)f.Length;
            }

            var newFile = $"{newFileDirectory}\\{f.LastWriteTime:yyyy-MM-dd-HH-mm}({_cnt}){Path.GetExtension(aFile)}";
            if (File.Exists(newFile))
            {
                aWriter.Write($"{f.FullName};");
                return;
            }

            Console.WriteLine("Loading \"{0}\" to \"{1}\" (No: {2}, Size/Total: {3}/{4} Mb)", f.FullName, toFolder, _cnt, Math.Round(ConvertBytesToMegabytes(f.Length)), Math.Round(ConvertBytesToMegabytes(_size)));

            f.CopyTo(newFile);
            aWriter.Write($"{f.FullName};");
        }

        /// <summary> Максимальная величина пула потоков </summary>
        private const int maxPoolSize = 10;

        /// <summary> Пул потоков </summary>
        private static readonly List<Task> taskPool = new List<Task>();

        /// <summary> Индикатор наличия ошибки </summary>
        /// <remarks> Флажок установится если в одном из потоков был сбой </remarks>
        private volatile bool wasError;

        /// <summary> Управление очередью процессов </summary>
        private void addFileToTaskPool(string aFile, StreamWriter aWriter)
        {
            while (!wasError)
            {
                if (taskPool.Count < maxPoolSize)
                {
                    taskPool.Add(Task.Factory.StartNew(() => copyFile(aFile, aWriter)));
                    break;
                }

                // Проверяем освободился ли в пуле потоков какой либо слот
                int index = Task.WaitAny(taskPool.ToArray(), 0);
                if (index > -1)
                {
                    wasError = taskPool[index].Exception != null;
                    if (wasError)
                    {
                        //var dtFile = taskPool[index].AsyncState as DtFile;

                        break;
                    }

                    taskPool[index] = Task.Factory.StartNew(() => copyFile(aFile, aWriter));
                    break;
                }

                // Если в пуле нет свободных слотов и мы дошли до максимального размера пула - ожидаем
                Thread.Sleep(200);
            }
        }

        private string GetSeason(DateTime date)
        {
            //using decimal to avoid any inaccuracy issues
            double value = (double)(date.Month + date.Day / 100M);   // <month>.<day(2 digit)>
            if (value < 3.21 || value >= 12.22)
            {
                return "Winter";
            }
            if (value < 6.21)
            {
                return "Spring";
            }
            if (value < 9.23)
            {
                return "Summer";
            }
            return "Autumn"; 
        }
        private double ConvertBytesToMegabytes(long bytes)
        {
            return (bytes / 1024f) / 1024f;
        }
    }
}