﻿/* Copyright (c) Microsoft Corporation. All rights reserved.
   Licensed under the MIT License. */

using RecurringIntegrationsScheduler.Common.Helpers;
using RecurringIntegrationsScheduler.Properties;
using RecurringIntegrationsScheduler.Settings;
using System;
using System.ComponentModel;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace RecurringIntegrationsScheduler.Forms
{
    public partial class ValidateConnection : Form
    {
        public ValidateConnection()
        {
            InitializeComponent();
        }

        public Instance Instance { get; set; }

        private void ValidateConnection_Load(object sender, EventArgs e)
        {
            axInstanceNameTextBox.Text = Instance.Name;
            aosURLTextBox.Text = Instance.AosUri;
            authEndPointTextBox.Text = Instance.AzureAuthEndpoint;
            tenantTextBox.Text = Instance.AadTenant;

            userComboBox.DataSource = Properties.Settings.Default.Users;
            userComboBox.ValueMember = null;
            userComboBox.DisplayMember = "Login";

            var applications = Properties.Settings.Default.AadApplications.Where(x => x.AuthenticationType == AuthenticationType.User);
            var applicationsBindingList = new BindingList<AadApplication>(applications.ToList());
            aadApplicationComboBox.DataSource = applicationsBindingList;
            aadApplicationComboBox.ValueMember = null;
            aadApplicationComboBox.DisplayMember = "Name";
        }

        private void ValidateButton_Click(object sender, EventArgs e)
        {
            messagesTextBox.Text = string.Empty;
            User user = null;

            if (userAuthRadioButton.Checked)
            {
                user = (User) userComboBox.SelectedItem;
                if (user == null)
                {
                    messagesTextBox.Text = Resources.Select_user_first;
                    return;
                }
            }

            var application = (AadApplication) aadApplicationComboBox.SelectedItem;
            if (application == null)
            {
                messagesTextBox.Text = Resources.Select_AAD_client_first;
                return;
            }

            Guid.TryParse(application.ClientId, out Guid aadClientGuid);

            var settings = new Common.JobSettings.DownloadJobSettings
            {
                RetryCount = 1,
                RetryDelay = 1,
                AadClientId = aadClientGuid,
                AadClientSecret = EncryptDecrypt.Decrypt(application.Secret),
                ActivityId = Guid.Empty,
                UseServiceAuthentication = serviceAuthRadioButton.Checked
            };

            if (Instance != null)
            {
                settings.AadTenant = Instance.AadTenant;
                settings.AosUri = Instance.AosUri.TrimEnd('/');
                settings.AzureAuthEndpoint = Instance.AzureAuthEndpoint;
                settings.UseADAL = Instance.UseADAL;
            }
            if (user != null)
            {
                settings.UserName = user.Login;
                settings.UserPassword = EncryptDecrypt.Decrypt(user.Password);
            }

            var httpClientHelper = new HttpClientHelper(settings);

            try
            {
                var response = Task.Run(async () =>
                {
                    var result = await httpClientHelper.GetRequestAsync(new Uri(settings.AosUri), true, CancellationToken.None);
                    return result;
                });
                response.Wait();

                messagesTextBox.Text += Resources.AAD_authentication_was_successful + Environment.NewLine;

                if (response.Result.StatusCode == HttpStatusCode.OK)
                {
                    messagesTextBox.Text += Resources.D365FO_instance_seems_to_be_up_and_running + Environment.NewLine;
                }
                else
                {
                    messagesTextBox.Text += $"{Resources.Warning_HTTP_response_status_for_D365FO_instance_is} {response.Result.StatusCode} {response.Result.ReasonPhrase}." + Environment.NewLine;
                }

                var checkAccess = httpClientHelper.GetDequeueUri();
                var checkAccessResponse = Task.Run(async () =>
                {
                    var result = await httpClientHelper.GetRequestAsync(checkAccess, true, CancellationToken.None);
                    return result;
                });
                checkAccessResponse.Wait();
                if (checkAccessResponse.Result.StatusCode == HttpStatusCode.Forbidden)
                {
                    if (settings.UseServiceAuthentication)
                    {
                        messagesTextBox.Text += Resources.AAD_application_is_not_mapped + Environment.NewLine;
                    }
                    else
                    {
                        messagesTextBox.Text += Resources.User_is_not_enabled + Environment.NewLine;
                    }
                }
                else if(checkAccessResponse.Result.StatusCode == HttpStatusCode.InternalServerError)
                {
                    //Internal error is expected as we are using empty guid as activity id. We only check access to API
                    //TODO: Investigate better approach
                    messagesTextBox.Text += Resources.Access_to_D365FO_instance_was_successful + Environment.NewLine;
                }
            }
            catch (Exception ex)
            {
                if (!string.IsNullOrEmpty(ex.Message))
                    messagesTextBox.Text += ex.Message + Environment.NewLine;
                var inner = ex;
                while (inner.InnerException != null)
                {
                    var innerMessage = inner.InnerException?.Message;
                    if (!string.IsNullOrEmpty(innerMessage))
                        messagesTextBox.Text += innerMessage + Environment.NewLine;
                    inner = inner.InnerException;
                }
            }
        }

        private void ServiceAuthRadioButton_CheckedChanged(object sender, EventArgs e)
        {
            aadApplicationComboBox.Text = "";
            var applications = serviceAuthRadioButton.Checked
                ? Properties.Settings.Default.AadApplications.Where(x => x.AuthenticationType == AuthenticationType.Service)
                : Properties.Settings.Default.AadApplications.Where(x => x.AuthenticationType == AuthenticationType.User);
            var applicationsBindingList = new BindingList<AadApplication>(applications.ToList());
            aadApplicationComboBox.DataSource = applicationsBindingList;
            aadApplicationComboBox.ValueMember = null;
            aadApplicationComboBox.DisplayMember = "Name";

            userComboBox.Enabled = !serviceAuthRadioButton.Checked;
        }
    }
}