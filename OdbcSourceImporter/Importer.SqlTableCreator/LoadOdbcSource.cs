using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Data.Odbc;
using System.Data;
using System.Data.SqlClient;

namespace Importer.DataLoader
{
    public class LoadOdbcSource
    {
        private string _sourceConnectionString = string.Empty;
        private string _destConnectionString = string.Empty;
        private string _sourceFilePath = string.Empty;
        private string _fileType = string.Empty;

        public LoadOdbcSource(string sourceConnectionString
            , string destConnectionString
            , string sourceFilePath
            , string fileType
            )
        {
            this._sourceConnectionString = sourceConnectionString;
            this._destConnectionString = destConnectionString;
            this._sourceFilePath = sourceFilePath;
            this._fileType = fileType;
        }

        public void LoadData()
        {
            string searchPattern = string.Format("*.{0}", _fileType);
            string[] sourceFiles = Directory.GetFiles(_sourceFilePath, searchPattern);

            List<string> tables = GetTableList();

            foreach (string sourceTableName in tables)
            {
                string fileName = string.Format("{0}.{1}",sourceTableName, _fileType);
                string fileFullName = Path.Combine(_sourceFilePath, fileName);

                string query = "SELECT * FROM [" + fileFullName + "]";
                if (sourceTableName == "offender")
                {
                    query = "SELECT full_name, first_name, mid_name, last_name, address, city, state, zip_code, phone_num, work_num, sex, race, height, weight, hair, eyes, birthdate, ss_number, scars_mark, clothing, nickname, maid_name, par_name, par_phone, dl_num, dl_state, exp_year, other_id, id_type, employer, occupation, offen_num, photo, transfered, caution, officer_id, date_entrd, exclusion, suspect, email, owner, owner_phon, manager, emer_phone, notes, keyholder, key_phone, sector, lalarm, lliquor, lsafe, lhazmat, nametype, sid, fbi, bofi, employee, tp_until FROM [" + fileFullName + "]";
                }

                string destTableName = string.Format("__Import_{0}", sourceTableName);

                using (OdbcConnection odbcConn = new OdbcConnection(_sourceConnectionString))
                {
                    odbcConn.Open();

                    OdbcDataAdapter adap = new OdbcDataAdapter(query, odbcConn);
                    DataSet ds = new DataSet();
                    adap.Fill(ds);
                    DataTable dt = ds.Tables[0];

                    int count = dt.Rows.Count;
                    if (count > 0)
                    {
                        using (SqlConnection sqlConn = new SqlConnection(_destConnectionString))
                        {
                            sqlConn.Open();
                            SqlTableCreator sqlCreator = new SqlTableCreator();
                            sqlCreator.Connection = sqlConn;
                            sqlCreator.DestinationTableName = destTableName;

                            sqlCreator.CreateFromDataTable(dt);

                            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(_destConnectionString))
                            {
                                bulkCopy.DestinationTableName = destTableName;
                                bulkCopy.WriteToServer(dt);
                            }

                        }
                    }

                }
            }
        }

        private List<string> GetTableList()
        {
            List<string> tables = new List<string>();
            using (OdbcConnection odbcConn = new OdbcConnection(_sourceConnectionString))
            {
                odbcConn.Open();

                DataTable tableschema = odbcConn.GetSchema(OdbcMetaDataCollectionNames.Tables);
                foreach (DataRow row in tableschema.Rows)
                {
                    string tableName = row["table_name"].ToString();
                    tables.Add(tableName);
                }
            }

            return tables;
        }
    }
}
