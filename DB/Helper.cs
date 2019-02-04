using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Security;
using System.Text;
using System.Threading.Tasks;

namespace DB
{
    class Helper
    {
        public static void BatchBulkCopy(DataTable dataTable, string connectionString, string DestinationTbl, int batchSize)
        {
            // Get the DataTable 
            Console.WriteLine("Insert data table to db table: " + DestinationTbl);
            DataTable dtInsertRows = dataTable;
            /*
            foreach (DataColumn column in dtInsertRows.Columns)
            {
                Console.Write("Item: ");
                Console.Write(column.ColumnName);
            }
            */

            using (SqlBulkCopy sbc = new SqlBulkCopy(connectionString, SqlBulkCopyOptions.KeepIdentity))
            {
                sbc.DestinationTableName = DestinationTbl;

                // Number of records to be processed in one go
                sbc.BatchSize = batchSize;

                foreach (DataColumn column in dataTable.Columns)
                {
                    sbc.ColumnMappings.Add(
                        new SqlBulkCopyColumnMapping(column.ColumnName, column.ColumnName));
                }

                // Finally write to server
                sbc.WriteToServer(dtInsertRows);
            }
        }

        public static string GetConnectionString(string serv, string uname, SecureString pass)
        {
            return string.Format("{0} User ID= {1}; Password= {2}", serv, uname, new System.Net.NetworkCredential("", pass).Password);
        }

        public static void WriteToDB(string db_server, string db_user, SecureString db_pass, DataTable data_source, string dest_table)
        {
            string conn = GetConnectionString(db_server, db_user, db_pass);
            Console.WriteLine("Connection string: " + conn);
            Helper.BatchBulkCopy(data_source, conn, dest_table, 5000);
        }

        public static DataTable SelectFromDB(string query, string db_server, string db_user, SecureString db_pass)
        {
            DataTable dt = new DataTable();
            string conn = GetConnectionString(db_server, db_user, db_pass);
            SqlConnection sqlConn = new SqlConnection(conn);
            SqlCommand cmd = new SqlCommand(query, sqlConn);
            sqlConn.Open();

            // create data adapter
            SqlDataAdapter da = new SqlDataAdapter(cmd);
            // this will query the database and return the result to your datatable
            da.Fill(dt);
            sqlConn.Close();
            da.Dispose();
            return dt;
        }

        public static void DeleteFromDB(string query, string db_server, string db_user, SecureString db_pass)
        {
            string conn = GetConnectionString(db_server, db_user, db_pass);
            SqlConnection sqlConn = new SqlConnection(conn);
            SqlCommand cmd = new SqlCommand(query, sqlConn);
            sqlConn.Open();
            cmd.ExecuteNonQuery();
            sqlConn.Close();
            /*
            SqlCommand sqlComm = new SqlCommand();
            sqlComm = sqlConn.CreateCommand();
            sqlComm.CommandText = @"DELETE FROM tableName WHERE conditionColumn='@conditionName'";
            sqlComm.Parameters.Add("@conditionName", SqlDbType.VarChar);
            sqlComm.Parameters["@conditionName"].Value = conditionSource;
            */
        }

        public static void UpdateInDB(string query, string db_server, string db_user, SecureString db_pass)
        {
            string conn = GetConnectionString(db_server, db_user, db_pass);
            SqlConnection sqlConn = new SqlConnection(conn);
            SqlCommand cmd = new SqlCommand(query, sqlConn);
            sqlConn.Open();
            cmd.ExecuteNonQuery();
            sqlConn.Close();
            /*
            SqlCommand sqlComm = new SqlCommand();
            sqlComm = sqlConn.CreateCommand();
            sqlComm.CommandText = @"UPDATE tableName SET paramColumn='@paramName' WHERE conditionColumn='@conditionName'";
            sqlComm.Parameters.Add("@paramName", SqlDbType.VarChar);
            sqlComm.Parameters["@paramName"].Value = paramSource;
            sqlComm.Parameters.Add("@conditionName", SqlDbType.VarChar);
            sqlComm.Parameters["@conditionName"].Value = conditionSource;
            */
        }
    }
}
