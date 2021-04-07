/* Copyright (c) Dreija OY. All rights reserved.
   Copyright (c) Microsoft Corporation. All rights reserved.
   Licensed under the MIT License. */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using log4net;
using Quartz.Util;

namespace RecurringIntegrationsScheduler.Common.Helpers
{
    public static class PowerShellHelper
    {
        /// <summary>
        /// The log
        /// </summary>
        private static readonly ILog Log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        /// <summary>
        /// Post request for files
        /// </summary>
        /// <param name="script">Path + file name of the PowerShell script</param>
        /// <param name="parameters">PowerShell script parameters</param>
        /// <returns></returns>
        public static void ExecutePowerShellScript(string script, Dictionary<string,string> parameters)
        {
            try
            {
                var parametersString = string.Join(" ", parameters.Select(kv => $"-{kv.Key} \"{kv.Value}\"").ToArray());

                var startInfo = new ProcessStartInfo()
                {
                    FileName = "powershell.exe",
                    Arguments = $"-NoProfile -ExecutionPolicy unrestricted -file \"{script}\" {parametersString}",
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    CreateNoWindow = true
                };

                var process = new Process { StartInfo = startInfo };
                process.Start();

                var output = process.StandardOutput.ReadToEnd();

                if (!output.IsNullOrWhiteSpace())
                {
                    Log.Info(output);
                }

                var error = process.StandardError.ReadToEnd();

                if (!error.IsNullOrWhiteSpace())
                {
                    Log.Error(error);
                }

                process.WaitForExit();
            }
            catch (Exception e)
            {
                Log.Error($"Error occurred while executing a PowerShell script ({script})", e);
            }
        }
    }
}
