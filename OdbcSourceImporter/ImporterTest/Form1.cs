using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Odbc;
using System.Data.SqlClient;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.Configuration;
using Importer.DataLoader;

namespace WindowsFormsApplication1
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
        }

        

        private void btnLoad_Click(object sender, EventArgs e)
        {
            string sourceDSN = "DSN=SanPablo";
            string destConnectionString = ConfigurationManager.ConnectionStrings["SQLDB"].ConnectionString;
            string sourceFilePath = @"C:\Dev\Conversion\SanPablo\ReportExec\ReportExec";
            LoadOdbcSource load = new LoadOdbcSource(sourceDSN, destConnectionString, sourceFilePath, "dbf");

            load.LoadData();

            MessageBox.Show("Done");
        }
    }
}
