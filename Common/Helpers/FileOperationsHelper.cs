﻿/* Copyright (c) Microsoft Corporation. All rights reserved.
   Licensed under the MIT License. */

using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using RecurringIntegrationsScheduler.Common.Contracts;
using RecurringIntegrationsScheduler.Common.Properties;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Net.Http;
using System.Text;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace RecurringIntegrationsScheduler.Common.Helpers
{
    public static class FileOperationsHelper
    {
        /// <summary>
        /// Deletes file specified by file path
        /// </summary>
        /// <param name="filePath">File path</param>
        /// <returns>Boolean with operation result</returns>
        public static void Delete(string filePath)
        {
            if (File.Exists(filePath))
            {
                File.Delete(filePath);
            }
        }

        /// <summary>
        /// Opens file as a file stream
        /// </summary>
        /// <param name="filePath">File path</param>
        /// <returns>Stream</returns>
        public static Stream Read(string filePath, FileShare fileShare = FileShare.ReadWrite)
        {
            if (File.Exists(filePath))
            {
                try
                {
                    return new FileStream(filePath,
                        FileMode.Open,
                        FileAccess.ReadWrite,
                        fileShare,
                        4096,
                        true);
                }
                catch (IOException)
                {                    
                }
            }
            return null;
         }

        /// <summary>
        /// Creates a file
        /// </summary>
        /// <param name="sourceStream">Source stream</param>
        /// <param name="filePath">Target file path</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Boolean with operation result</returns>
        public static async Task CreateAsync(Stream sourceStream, string filePath, CancellationToken cancellationToken)
        {
            var targetDirectoryName = Path.GetDirectoryName(filePath);
            if (targetDirectoryName == null)
                throw new DirectoryNotFoundException();

            Directory.CreateDirectory(targetDirectoryName);
            using var fileStream = File.Create(filePath);
            sourceStream.Seek(0, SeekOrigin.Begin);
            await sourceStream.CopyToAsync(fileStream, 81920, cancellationToken);
        }

        /// <summary>
        /// Searches for source files in target path and returns list of dataMessage objects
        /// </summary>
        /// <param name="messageStatus">Data message status for found files</param>
        /// <param name="path">Path to search</param>
        /// <param name="searchPatterns">File pattern</param>
        /// <param name="searchOption">Search option</param>
        /// <param name="sortBy">Sort by field</param>
        /// <param name="reverse">Order of files</param>
        /// <returns>List of dataMessage objects</returns>
        public static IEnumerable<DataMessage> GetFiles(MessageStatus messageStatus, string path, string searchPatterns = "*.*",
            SearchOption searchOption = SearchOption.AllDirectories, OrderByOptions sortBy = OrderByOptions.Created,
            bool reverse = false)
        {
            var dir = new DirectoryInfo(path);
            var sortByProperty = string.Empty;
            switch (sortBy)
            {
                case OrderByOptions.Created:
                    sortByProperty = @"CreationTimeUtc";
                    break;

                case OrderByOptions.Modified:
                    sortByProperty = @"LastWriteTimeUtc";
                    break;

                case OrderByOptions.Size:
                    sortByProperty = @"Length";
                    break;

                case OrderByOptions.FileName:
                    sortByProperty = @"FullName";
                    break;
            }

            foreach (FileInfo fileName in dir.EnumerateFiles(searchPatterns, searchOption).AsQueryable().Sort(sortByProperty, reverse)
            )
            {
                var dataMessage = new DataMessage
                {
                    Name = fileName.Name,
                    FullPath = fileName.FullName,
                    MessageStatus = messageStatus
                };
                yield return dataMessage;
            }
        }

        /// <summary>
        /// Searches for status files in target path and returns list of dataMessage objects
        /// </summary>
        /// <param name="messageStatus">Data message status for found files</param>
        /// <param name="path">Path to search</param>
        /// <param name="searchPatterns">File pattern</param>
        /// <param name="searchOption">Search option</param>
        /// <param name="sortBy">Sort by field</param>
        /// <param name="reverse">Order of files</param>
        /// <returns>List of dataMessage objects</returns>
        public static IEnumerable<DataMessage> GetStatusFiles(MessageStatus messageStatus, string path,
            string searchPatterns = "*.Status", SearchOption searchOption = SearchOption.AllDirectories,
            OrderByOptions sortBy = OrderByOptions.Created, bool reverse = false)
        {
            var dir = new DirectoryInfo(path);
            var sortByProperty = string.Empty;
            switch (sortBy)
            {
                case OrderByOptions.Created:
                    sortByProperty = @"CreationTimeUtc";
                    break;

                case OrderByOptions.Modified:
                    sortByProperty = @"LastWriteTimeUtc";
                    break;

                case OrderByOptions.Size:
                    sortByProperty = @"Length";
                    break;

                case OrderByOptions.FileName:
                    sortByProperty = @"FullName";
                    break;
            }

            foreach (FileInfo fileName in dir.EnumerateFiles(searchPatterns, searchOption).AsQueryable().Sort(sortByProperty, reverse)
            )
            {
                DataMessage dataMessage;
                using (var file = File.OpenText(fileName.FullName))
                {
                    var serializer = new JsonSerializer();
                    dataMessage = (DataMessage) serializer.Deserialize(file, typeof(DataMessage));
                }
                yield return dataMessage;
            }
        }

        /// <summary>
        /// Searches for all subfolders of given path
        /// </summary>
        /// <param name="directory">Root directory</param>
        /// <returns>List of subfolders' paths</returns>
        public static IEnumerable<string> FindAllSubfolders(string directory)
        {
            var allSubfolders = new List<string>();
            foreach (var d in Directory.GetDirectories(directory))
            {
                allSubfolders.Add(d);
                allSubfolders.AddRange(FindAllSubfolders(d));
            }
            return allSubfolders;
        }

        /// <summary>
        /// Extracts content of data package zip archive
        /// </summary>
        /// <param name="filePath">File path of data package</param>
        /// <param name="deletePackage">Flag whether to delete zip file</param>
        /// <param name="addTimestamp">Flag whether to add timestamp to extracted file name</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>List of unzipped file names</returns>
        public static async Task<List<string>> UnzipPackageAsync(string filePath, bool deletePackage, bool addTimestamp, CancellationToken cancellationToken)
        {
            var unzippedFiles = new List<string>();

            if (File.Exists(filePath))
            {
                using (var zip = ZipFile.OpenRead(filePath))
                {
                    foreach (var entry in zip.Entries)
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        if ((entry.Length == 0) || (entry.FullName == "Manifest.xml") ||
                            (entry.FullName == "PackageHeader.xml"))
                            continue;

                        string fileName;

                        if (addTimestamp)
                            fileName =
                                Path.Combine(Path.GetDirectoryName(filePath) ?? throw new InvalidOperationException(),
                                    Path.GetFileNameWithoutExtension(filePath) ?? throw new InvalidOperationException()) + "-" + entry.FullName;
                        else
                            fileName = Path.Combine(Path.GetDirectoryName(filePath) ?? throw new InvalidOperationException(), entry.FullName);

                        if (File.Exists(fileName) && addTimestamp) 
                            continue;

                        using var zipStream = entry.Open();
                        using var fileStream = new FileStream(fileName, FileMode.Create);
                        await zipStream.CopyToAsync(fileStream, 81920, cancellationToken);
                        unzippedFiles.Add(fileName);
                    }
                }
                if (deletePackage)
                    File.Delete(filePath);
            }

            return unzippedFiles;
        }

        /// <summary>
        /// Moves source file between folders and deletes status files when needed
        /// </summary>
        /// <param name="sourceFilePath">Source file path</param>
        /// <param name="targetFilePath">Target file path</param>
        /// <param name="deleteStatusFile">Flag whether to delete status file</param>
        /// <param name="statusFileExtension">Status file extension</param>
        public static void MoveDataToTarget(string sourceFilePath, string targetFilePath, bool deleteStatusFile = false, string statusFileExtension = ".Status")
        {
            Move(sourceFilePath, targetFilePath);

            //Now status file
            if (!deleteStatusFile)
            {
                var sourceStatusFile = Path.Combine(Path.GetDirectoryName(sourceFilePath) ?? throw new InvalidOperationException(), Path.GetFileNameWithoutExtension(sourceFilePath) + statusFileExtension);
                var targetStatusFile = Path.Combine(Path.GetDirectoryName(targetFilePath) ?? throw new InvalidOperationException(), Path.GetFileNameWithoutExtension(targetFilePath) + statusFileExtension);
                Move(sourceStatusFile, targetStatusFile);
            }
            else
            {
                var sourceStatusFile = Path.Combine(Path.GetDirectoryName(sourceFilePath) ?? throw new InvalidOperationException(), Path.GetFileNameWithoutExtension(sourceFilePath) + statusFileExtension);
                Delete(sourceStatusFile);
            }
        }

        /// <summary>
        /// Moves source file between folders
        /// </summary>
        /// <param name="sourceFilePath">Source file path</param>
        /// <param name="targetFilePath">Target file path</param>
        /// <returns>Boolean with operation result</returns>
        public static void Move(string sourceFilePath, string targetFilePath)
        {
            new FileInfo(targetFilePath).Directory.Create();//Create subfolders if necessary
            if (File.Exists(targetFilePath))
            {
                File.Delete(targetFilePath);
            }
            File.Move(sourceFilePath, targetFilePath);
        }

        /// <summary>
        /// Creates status file for data message
        /// </summary>
        /// <param name="dataMessage">dataMessage object</param>
        /// <param name="statusFileExtension">Status file extension</param>
        /// <param name="cancellationToken">Cancellation token</param>
        public static async Task WriteStatusFileAsync(DataMessage dataMessage, string statusFileExtension, CancellationToken cancellationToken)
        {
            if (dataMessage == null)
            {
                return;
            }
            var statusData = JsonConvert.SerializeObject(dataMessage, Formatting.Indented, new StringEnumConverter());

            using var statusFileMemoryStream = new MemoryStream(Encoding.Default.GetBytes(statusData));
            await CreateAsync(statusFileMemoryStream, Path.Combine(Path.GetDirectoryName(dataMessage.FullPath) ?? throw new InvalidOperationException(), Path.GetFileNameWithoutExtension(dataMessage.FullPath) + statusFileExtension), cancellationToken);
        }

        /// <summary>
        /// Creates status log file for data message
        /// </summary>
        /// <param name="targetDataMessage">target dataMessage object</param>
        /// <param name="httpResponse">httpResponse object</param>
        /// <param name="statusFileExtension">Status file extension</param>
        /// <param name="cancellationToken">Cancellation token</param>
        public static async Task WriteStatusLogFileAsync(DataMessage targetDataMessage, HttpResponseMessage httpResponse, string statusFileExtension, CancellationToken cancellationToken)
        {
            if (targetDataMessage == null || httpResponse == null)
            {
                return;
            }
            var logFilePath = Path.Combine(Path.GetDirectoryName(targetDataMessage.FullPath) ?? throw new InvalidOperationException(), Path.GetFileNameWithoutExtension(targetDataMessage.FullPath) + statusFileExtension);
            var logData = JsonConvert.SerializeObject(httpResponse, Formatting.Indented, new StringEnumConverter());

            using var logMemoryStream = new MemoryStream(Encoding.Default.GetBytes(logData));
            await CreateAsync(logMemoryStream, logFilePath, cancellationToken);
        }

        /// <summary>
        /// Creates status log file for data message
        /// </summary>
        /// <param name="jobStatusDetail">DataJobStatusDetail object</param>
        /// <param name="targetDataMessage">target dataMessage object</param>
        /// <param name="httpResponse">httpResponse object</param>
        /// <param name="statusFileExtension">Status file extension</param>
        /// <param name="cancellationToken">Cancellation token</param>
        public static async Task WriteStatusLogFileAsync(DataJobStatusDetail jobStatusDetail, DataMessage targetDataMessage, HttpResponseMessage httpResponse, string statusFileExtension, CancellationToken cancellationToken)
        {
            if (targetDataMessage == null)
            {
                return;
            }
            var logFilePath = Path.Combine(Path.GetDirectoryName(targetDataMessage.FullPath) ?? throw new InvalidOperationException(), Path.GetFileNameWithoutExtension(targetDataMessage.FullPath) + statusFileExtension);
            string logData;

            if (null != jobStatusDetail)
            {
                logData = JsonConvert.SerializeObject(jobStatusDetail, Formatting.Indented, new StringEnumConverter());
            }
            else if (null != httpResponse)
            {
                logData = JsonConvert.SerializeObject(httpResponse, Formatting.Indented, new StringEnumConverter());
            }
            else
            {
                logData = Resources.Unknown_error;
            }

            using var logMemoryStream = new MemoryStream(Encoding.Default.GetBytes(logData));
            await CreateAsync(logMemoryStream, logFilePath, cancellationToken);
        }
    }
}