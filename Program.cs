using System;
using System.IO;
using System.Configuration;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.SqlClient;


namespace WebportDownload
{
    class Webport
    {
        static String connStrKildeSql = "server=source;Initial Catalog=WebportCmProject;Integrated Security=SSPI";
        static String connStrMaalSql = "server=dest;Initial Catalog=Webport;Integrated Security=SSPI";
        private SqlConnection connKildeSql; // Source
        private SqlConnection connMaalSql; // Dest.

        static String CompanyID = @"HBE";
       // Source database procedure for retreival of data
   //     private string cmd_Retrieve_FormList = "dbo.Cm_Project_Export_FormList"); //_ByCompanyId"; // 1 rec per underøsklese - formID som finnes. Startpunkt for resten av spørringene.
        private string cmd_Retrieve_Questionnaires = "dbo.Cm_Project_Export_FormList_ByCompanyId"; // returenerer 1 rec per spørreundersøkelse ID, Guid, Title
        private string cmd_Retrieve_Questions = "dbo.Cm_Project_Export_FormFields_ByFormId";
        private string cmd_Retrieve_Categories = "dbo.Cm_Project_Export_SelectionListDetail_ByFormId";
        private string cmd_Retrieve_Createset = "dbo.CM_Project_Export_CreateSet_ByFormId";
        private string cmd_Retrieve_Answers = "dbo.CM_Project_Export_PatientForm_ByFormId";
        private string cmd_Retrieve_Demographics = "dbo.CM_Project_Export_Demographics_ByFormId";
        private string cmd_Mark_Exported = "dbo.CM_Project_Export_MarkExported_ByFormId";
        private string cmd_Retrieve_Unfinished_Sets = "Select FormId, CompanyId, ExportGuid, ExportSetId from dbo.webportCreateSet where isnull(DownloadComplete, 0)!=1";

        // Calls for dest.-database processing, past bulk-import
        private string cmd_Update_Questionnaires = "Exec dbo.Oppdater_undersøkelser_fra_import"; // Oppdaterer maal-database etter overføring av data
        private string cmd_Update_Questions = "Exec dbo.Oppdater_spørsmål_fra_import";
        private string cmd_Update_Categories = "Exec dbo.Oppdater_svaralternativ_fra_import";
        private string cmd_Update_Answers = "Exec dbo.Oppdater_svar_fra_import";
        private string cmd_Update_CreateSets = "Exec dbo.Oppdater_Createset_fra_import";
        private string cmd_Update_Demographics = "Exec dbo.Oppdater_bakgrunnsdata_fra_import";

        // Table Names in dest. database
        private string TableName_Undersokelse_Dest = "dbo.Undersøkelse_import";
        private string TableName_Sporsmaal_Dest = "dbo.spørsmål_import";
        private string TableName_Svaralternativ_Dest = "dbo.svaralternativ_import";
        private string TableName_Svar_Dest = "dbo.svar_import";
        private string TableName_CreateSet_Dest = "dbo.webportCreateSet_import";
        private string TableName_Demographics_Dest = "dbo.Bakgrunnsdata_import";

        // Datatable-objects for in-memory-processing
        private System.Data.DataTable dt_Undersokelser = null;
        private System.Data.DataTable dt_Sporsmaal = null;
        private System.Data.DataTable dt_Svaralternativ = null;
        private System.Data.DataTable dt_OldCreateSet = null;
        private System.Data.DataTable dt_CreateSet = null;
        private System.Data.DataTable dt_Svar = null;
        private System.Data.DataTable dt_Bakgrunnsdata = null;

        // Config-identificators
        static String settings_Connection_Source = @"sourceConnection";
        static String settings_Connection_Dest = @"destConnection";
        static String settings_CompanyId = @"CompanyId";

        // Read user config
        static void Get_Configuration()
        {
            try
            {
                System.Configuration.AppSettingsReader appReader = new AppSettingsReader();
                string config_connStrMaalSql = (string)appReader.GetValue(settings_Connection_Dest, typeof(String));
                if (config_connStrMaalSql != null) connStrMaalSql = config_connStrMaalSql;
                string config_connStrKildeSql = (string)appReader.GetValue(settings_Connection_Source, typeof(String));
                if (config_connStrKildeSql != null) connStrKildeSql = config_connStrKildeSql;
                string config_company = (string)appReader.GetValue(settings_CompanyId, typeof(String));
                if (config_company != null) CompanyID = config_company;
            }
            catch
            { }
        }

        // SQL Execution for dest. database
        public int Execute_Command(string command, SqlConnection conn)
        {
            try
            {
                System.Data.SqlClient.SqlCommand cmd = new SqlCommand(command, conn);
                cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                return -1;
            }
            return 0;
        }

        // Logging (in dest. SQL Server)
        void WriteLog(int code, string msg)
        {
            string s = @"Exec dbo.InsertLog " + code.ToString() + ", '" + msg + "'";
            Execute_Command(s, connMaalSql);
        }

        public void Init_Maalserver() // Init dest.
        {
            connMaalSql = new SqlConnection(connStrMaalSql);
            connMaalSql.Open();
        }

        public void Init_Kildeserver() // Init source
        {
            connKildeSql = new SqlConnection(connStrKildeSql);
            connKildeSql.Open();
        }
        public void TruncateDestTables()
        {
            System.Data.SqlClient.SqlCommand cmd = new SqlCommand("Exec dbo.Truncate_Import_Tables", connMaalSql);
            cmd.ExecuteNonQuery();
        }

        public void CloseConnections()
        {
            connKildeSql.Close();
            connMaalSql.Close();
        }

        // Retrieve data fom SOURCE. No input-table, only output (dt_Dest)
        public void Retrieve_Data(ref DataTable dt_Dest, string command, bool company)
        {
            DataTable dtSchema = null;
            List<System.Data.DataColumn> listCols = null;

            SqlCommand cmd = new SqlCommand();
            cmd.Connection = connKildeSql;
            cmd.CommandType = System.Data.CommandType.StoredProcedure;
            cmd.CommandText = command;
            if (company)
            {
                SqlParameter parForm_Company = new SqlParameter();
                parForm_Company.ParameterName = "@CompanyId";
                parForm_Company.Value = CompanyID;
                cmd.Parameters.Add(parForm_Company);
            }
            dt_Dest = new DataTable();
                       
            SqlDataReader reader = cmd.ExecuteReader();
            if (dtSchema == null) // Get table def. - Create column LIST
            {
                dtSchema = reader.GetSchemaTable();
                if (dtSchema != null) // On success 
                {
                    listCols = new List<System.Data.DataColumn>();
                    foreach (DataRow drow in dtSchema.Rows) // Find def. of columns.
                    {
                        string columnName = System.Convert.ToString(drow["ColumnName"]);
                        DataColumn column = new DataColumn(columnName, (Type)(drow["DataType"]));
                        // column.Unique = (bool)drow["IsUnique"];
                        column.AllowDBNull = (bool)drow["AllowDBNull"];
                        // column.AutoIncrement = (bool)drow["IsAutoIncrement"];
                        listCols.Add(column);
                        dt_Dest.Columns.Add(column);
                    }
                }
            }
            while (reader.Read())
            { // Fetch data
                DataRow dataRow = dt_Dest.NewRow();
                for (int i = 0; i < listCols.Count; i++)
                {
                    dataRow[((DataColumn)listCols[i])] = reader[i];
                }
                dt_Dest.Rows.Add(dataRow);
            }
            reader.Close();
        }

        // Retrieve data fom SOURCE. Input table defines what to retrieve. company = identity for "customer/installation" in source.
        public void Retrieve_Data(ref DataTable dt_Dest, ref DataTable dt_Input, string command, bool company) 
        {
            String idnr;
            DataTable dtSchema = null;
            List<System.Data.DataColumn> listCols = null;

            SqlCommand cmd = new SqlCommand();
            cmd.Connection = connKildeSql;
            cmd.CommandType = System.Data.CommandType.StoredProcedure;
            cmd.CommandText = command; 
            SqlParameter parFormID = new SqlParameter();
            parFormID.ParameterName = "@FormId";
            cmd.Parameters.Add(parFormID);
            if (company)
            {
                SqlParameter parForm_Company = new SqlParameter();
                parForm_Company.ParameterName = "@CompanyId";
                cmd.Parameters.Add(parForm_Company);
                parForm_Company.Value = CompanyID;
            }
            dt_Dest = new DataTable();
            foreach (DataRow dr in dt_Input.Rows)
            {
                idnr = dr["Id"].ToString();
                parFormID.Value = idnr;
                SqlDataReader reader = cmd.ExecuteReader();
                if (dtSchema == null) // Hent skjema - Lag kolonneliste
                {
                    dtSchema = reader.GetSchemaTable();
                    if (dtSchema != null) // Hvis suksess 
                    {
                        listCols = new List<System.Data.DataColumn>();
                        foreach (DataRow drow in dtSchema.Rows) // Finn definisjoner av kolonner
                        {
                            string columnName = System.Convert.ToString(drow["ColumnName"]);
                            DataColumn column = new DataColumn(columnName, (Type)(drow["DataType"]));
                            // column.Unique = (bool)drow["IsUnique"];
                            column.AllowDBNull = (bool)drow["AllowDBNull"];
                            // column.AutoIncrement = (bool)drow["IsAutoIncrement"];
                            listCols.Add(column);
                            dt_Dest.Columns.Add(column);
                        }
                    }
                }
                while (reader.Read())
                {
                    DataRow dataRow = dt_Dest.NewRow();
                    for (int i = 0; i < listCols.Count; i++)
                    {
                        dataRow[((DataColumn)listCols[i])] = reader[i];
                    }
                    dt_Dest.Rows.Add(dataRow);
                }
                reader.Close();
            }
        }

        // Retrieve data to dt_Dest by SQL command.
        public void Select_From_TargetDB(ref DataTable dt_Dest, string cmdText)
        {

            SqlCommand cmd = new SqlCommand();
            cmd.Connection = connMaalSql;
            cmd.CommandType = System.Data.CommandType.Text;
            cmd.CommandText = cmdText;

            DataTable dtSchema = null;
            List<System.Data.DataColumn> listCols = null;

            dt_Dest = new DataTable();
            SqlDataReader reader = cmd.ExecuteReader();
            if (dtSchema == null) // On success, create column list 
            {
                dtSchema = reader.GetSchemaTable();
                if (dtSchema != null) // Hvis suksess 
                {
                    listCols = new List<System.Data.DataColumn>();
                    foreach (DataRow drow in dtSchema.Rows) // Get column defs.
                    {
                        string columnName = System.Convert.ToString(drow["ColumnName"]);
                        DataColumn column = new DataColumn(columnName, (Type)(drow["DataType"]));
                        column.AllowDBNull = (bool)drow["AllowDBNull"];
                        // column.AutoIncrement = (bool)drow["IsAutoIncrement"];
                        listCols.Add(column);
                        dt_Dest.Columns.Add(column);
                    }
                }
            }
            while (reader.Read()) // Fetch data
            {
                DataRow dataRow = dt_Dest.NewRow();
                for (int i = 0; i < listCols.Count; i++)
                {
                    dataRow[((DataColumn)listCols[i])] = reader[i];
                }
                dt_Dest.Rows.Add(dataRow);
            }
            reader.Close();
        }

        // Retrieval of data by the product of two input tables(dt_input*dt_Set)
        // NOT IN USE
        public void Retrieve_Data(ref DataTable dt_Dest, ref DataTable dt_Input, ref DataTable dt_Set, string command, bool company)
        {
            String idnr, exportGuid;
            DataTable dtSchema = null;
            List<System.Data.DataColumn> listCols = null;

            SqlCommand cmd = new SqlCommand();
            cmd.Connection = connKildeSql;
            cmd.CommandType = System.Data.CommandType.StoredProcedure;
            cmd.CommandText = command;

            SqlParameter parExportGuid = new SqlParameter();
            parExportGuid.ParameterName = "@ExportGuid";
            cmd.Parameters.Add(parExportGuid);

            SqlParameter parFormID = new SqlParameter();
            parFormID.ParameterName = "@FormId";
            cmd.Parameters.Add(parFormID);

            if (company)
            {
                SqlParameter parForm_Company = new SqlParameter();
                parForm_Company.ParameterName = "@CompanyId";
                cmd.Parameters.Add(parForm_Company);
                parForm_Company.Value = CompanyID;
            }
            dt_Dest = new DataTable();
            foreach (DataRow dsetRow in dt_Set.Rows)
            {
                exportGuid = dsetRow["ExportGuid"].ToString();
                parExportGuid.Value = exportGuid;
                foreach (DataRow dr in dt_Input.Rows)
                {
                    idnr = dr["Id"].ToString();
                    parFormID.Value = idnr;
                    SqlDataReader reader = cmd.ExecuteReader();
                    if (dtSchema == null) // Create column list
                    {
                        dtSchema = reader.GetSchemaTable();
                        if (dtSchema != null) // On success 
                        {
                            listCols = new List<System.Data.DataColumn>();
                            foreach (DataRow drow in dtSchema.Rows) // Finn definisjoner av kolonner
                            {
                                string columnName = System.Convert.ToString(drow["ColumnName"]);
                                DataColumn column = new DataColumn(columnName, (Type)(drow["DataType"]));
                                // column.Unique = (bool)drow["IsUnique"];
                                column.AllowDBNull = (bool)drow["AllowDBNull"];
                                // column.AutoIncrement = (bool)drow["IsAutoIncrement"];
                                listCols.Add(column);
                                dt_Dest.Columns.Add(column);
                            }
                        }
                    }
                    while (reader.Read())
                    {
                        DataRow dataRow = dt_Dest.NewRow();
                        for (int i = 0; i < listCols.Count; i++)
                        {
                            dataRow[((DataColumn)listCols[i])] = reader[i];
                        }
                        dt_Dest.Rows.Add(dataRow);
                    }
                    reader.Close();
                }
            }
        }

        // Retrieval of collected responses.
        public void Retrieve_DataAnswers(ref DataTable dt_Dest, ref DataTable dt_Set, string command, bool company)
        {
            DataTable dtSchema = null;
            List<System.Data.DataColumn> listCols = null;
            SqlCommand cmd = new SqlCommand();
            cmd.Connection = connKildeSql;
            cmd.CommandType = System.Data.CommandType.StoredProcedure;
            cmd.CommandText = command;

            SqlParameter parFormID = new SqlParameter();
            parFormID.ParameterName = "@FormId";
            cmd.Parameters.Add(parFormID);

            SqlParameter parCompanyId = new SqlParameter();
            if (company)
            {
                parCompanyId.ParameterName = "@CompanyId";
                cmd.Parameters.Add(parCompanyId);
            }

            SqlParameter parExportGuid = new SqlParameter();
            parExportGuid.ParameterName = "@ExportGuid";
            cmd.Parameters.Add(parExportGuid);

            dt_Dest = new DataTable();
            foreach (DataRow dsetRow in dt_Set.Rows)
            { 
                parFormID.Value = dsetRow["FormId"].ToString();
                if (company) parCompanyId.Value = dsetRow["CompanyId"].ToString();
                parExportGuid.Value = dsetRow["ExportGuid"].ToString();
                SqlDataReader reader = cmd.ExecuteReader();
                if (dtSchema == null) // Create column list
                {
                    dtSchema = reader.GetSchemaTable();
                    if (dtSchema != null) // On success 
                    {
                        listCols = new List<System.Data.DataColumn>();
                        foreach (DataRow drow in dtSchema.Rows) // Column defs.
                        {
                            string columnName = System.Convert.ToString(drow["ColumnName"]);
                            DataColumn column = new DataColumn(columnName, (Type)(drow["DataType"]));
                            // column.Unique = (bool)drow["IsUnique"];
                            column.AllowDBNull = (bool)drow["AllowDBNull"];
                            // column.AutoIncrement = (bool)drow["IsAutoIncrement"];
                            listCols.Add(column);
                            dt_Dest.Columns.Add(column);
                        }
                    }
                }
                while (reader.Read())
                {
                    DataRow dataRow = dt_Dest.NewRow();
                    for (int i = 0; i < listCols.Count; i++)
                    {
                        dataRow[((DataColumn)listCols[i])] = reader[i];
                    }
                    dt_Dest.Rows.Add(dataRow);
                }
                reader.Close();
            }
        }

        // Write data from datatable to dest. server
        public void Bulk_Insert_Table(DataTable dt, string destTable)
        {
            SqlBulkCopy bulkWrite = new SqlBulkCopy(connMaalSql);
            foreach (DataColumn c in dt.Columns)
            {
                bulkWrite.ColumnMappings.Add(c.ColumnName, c.ColumnName);
            }
            bulkWrite.DestinationTableName = destTable;
            try
            {
                bulkWrite.WriteToServer(dt);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                WriteLog(-1, ex.Message);
            }
        }

        // Send Confirmation receipt to source server: "set" of questionnaires is successfully transferred/downloaded.
        void UpdateSetCompleted(DataTable dt)
        {
            String ExportSetid; string cmdString;
            foreach (DataRow dr in dt.Rows)
            {
                ExportSetid = dr["ExportSetId"].ToString();
                cmdString = @"Exec dbo.SetCreatesetCompleted " + ExportSetid.ToString() ;
                Execute_Command(cmdString, connMaalSql);
            }
        }

        public Webport()
        {
        }

        // Main 
        public int Run()
        { 
           int returnValue = 0;
           try
            {
                //WriteLog(returnValue, "Start Program"); - for debugging process..
                Get_Configuration(); 
                Init_Maalserver();         //     WriteLog(returnValue, "Init Dest. server");
                Init_Kildeserver();        //        WriteLog(returnValue, "Init source server");         
                TruncateDestTables();   
                // Get list of active Quest.
                Retrieve_Data(ref dt_Undersokelser, cmd_Retrieve_Questionnaires, true);  // Use CompanyID=true, value from config.
                Bulk_Insert_Table(dt_Undersokelser, TableName_Undersokelse_Dest);
                returnValue = Execute_Command(cmd_Update_Questionnaires, connMaalSql); 
                WriteLog(returnValue, "Fetched questionnaires");

                // Get Questions for the Questionnaires 
                Retrieve_Data(ref dt_Sporsmaal, ref dt_Undersokelser, cmd_Retrieve_Questions, false);
                Bulk_Insert_Table(dt_Sporsmaal, TableName_Sporsmaal_Dest);
                returnValue = Execute_Command(cmd_Update_Questions, connMaalSql);
                WriteLog(returnValue, "Fetched questions");
                // Get categories for questions
                Retrieve_Data(ref dt_Svaralternativ, ref dt_Undersokelser, cmd_Retrieve_Categories, false);
                Bulk_Insert_Table(dt_Svaralternativ, TableName_Svaralternativ_Dest);
                returnValue = Execute_Command(cmd_Update_Categories, connMaalSql);
                WriteLog(returnValue, "Fetched categories");

                // In case of prev. failure on "set" download, retry:
                Select_From_TargetDB(ref dt_OldCreateSet, cmd_Retrieve_Unfinished_Sets);
                if (dt_OldCreateSet.Rows.Count > 0)
                {
                    DataTable dt_svar_oldSets = null;
                    Retrieve_DataAnswers(ref dt_svar_oldSets, ref dt_OldCreateSet, cmd_Retrieve_Answers, true);
                    Bulk_Insert_Table(dt_svar_oldSets, TableName_Svar_Dest);
                    returnValue = Execute_Command(cmd_Update_Answers, connMaalSql);
                    WriteLog(returnValue, "Old set successfully downloaded");
                    if (returnValue == 0)
                    {
                        DataTable dt_markedExported_oldSets = null;
                        UpdateSetCompleted(dt_OldCreateSet);
                        Retrieve_DataAnswers(ref dt_markedExported_oldSets, ref dt_OldCreateSet, cmd_Mark_Exported, false);  // kvittering nedlastet
                    }
                }

                // Fetch new "Set(s)" (CreateSet), for download.
                Retrieve_Data(ref dt_CreateSet, ref dt_Undersokelser, cmd_Retrieve_Createset, true);  // BrukCompanyID=true
                Bulk_Insert_Table(dt_CreateSet, TableName_CreateSet_Dest);
                returnValue = Execute_Command(cmd_Update_CreateSets, connMaalSql);
                WriteLog(returnValue, "Fetched exportSets");

                // Get responses
                Retrieve_DataAnswers(ref dt_Svar, ref dt_CreateSet, cmd_Retrieve_Answers, true);
                Bulk_Insert_Table(dt_Svar, TableName_Svar_Dest);
                returnValue = Execute_Command(cmd_Update_Answers, connMaalSql);
                WriteLog(returnValue, "Set completed");
                if (returnValue == 0)
                {
                    DataTable dt_markExported = null;
                    UpdateSetCompleted(dt_CreateSet);                // mark "CreateSet" as completed
                    Retrieve_DataAnswers(ref dt_markExported, ref dt_CreateSet, cmd_Mark_Exported, true);// Receipt to source server - set succesfully downloaded.
                    WriteLog(returnValue, "New Set downloaded");
                }
                // get background data (Location (in hospital), Age group , Gender.)
                WriteLog(returnValue, "Start get demographics");
                Retrieve_Data(ref dt_Bakgrunnsdata, ref dt_Undersokelser, cmd_Retrieve_Demographics, false);  // BrukCompanyID=false Endret av TL.
                Bulk_Insert_Table(dt_Bakgrunnsdata, TableName_Demographics_Dest);
                returnValue = Execute_Command(cmd_Update_Demographics, connMaalSql);
                WriteLog(returnValue, "Completed - get demographics");

                CloseConnections();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                returnValue = -1;
                WriteLog(returnValue, ex.Message);
            }
            return returnValue;
        }
    }
    class Program
    {
        static int  Main(string[] args)
        {
            Webport w = new Webport();
            return w.Run();
        }
    }
}
