using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using AddressMatch;
using System.Data;
using System.Configuration;
using System.Runtime.Remoting.Messaging;

namespace AddressMatchMain
{
    public class Program
    {

        //public static DataSet DSToT1 = new DataSet();
        //public static DataSet DSFromT1 = new DataSet();


        //public static DataSet DSToT2 = new DataSet();
        //public static DataSet DSFromT2 = new DataSet();

        static void Main(string[] args)
        {



            

            DataSet DSCount = new DataSet();

            

            string tablename = ConfigurationManager.AppSettings["TableName"].ToString();


            string sqlcount = string.Format(@"select count(tb_addr_id_num) as totalcount,max(tb_addr_id_num) as maxnumid,min(tb_addr_id_num) as minnumid 
                                from {0}",tablename);
            DSCount = SimpleDataHelper.Query(SimpleDataHelper.MSConnectionString, sqlcount);

            int tc = Convert.ToInt32(DSCount.Tables[0].Rows[0][0].ToString());
            int maxc = Convert.ToInt32(DSCount.Tables[0].Rows[0][1].ToString());
            int minc = Convert.ToInt32(DSCount.Tables[0].Rows[0][2].ToString());

            int cyclecount = (tc / 5000);

            for (int i = 0; i <= cyclecount; i++)
            {
                try
                {
                    DataSet DSToT1 = new DataSet();
                    DataSet DSToT2 = new DataSet();
                    DataSet DSToT3 = new DataSet();
                    DataSet DSToT4 = new DataSet();
                    DataSet DSToT5 = new DataSet();
                    DataSet DSFromT = new DataSet();

                    string addset = (i * 5000).ToString();

                    string sql1 = string.Format("select tb_addr_id,tb_addr,tb_addr_id_num from {0}  with(nolock) where tb_addr_id_num>0+{2} and tb_addr_id_num<={1}+1000+{2}-1", tablename, minc.ToString(), addset);
                    DSToT1 = SimpleDataHelper.Query(SimpleDataHelper.MSConnectionString, sql1);

                    string sql2 = string.Format("select tb_addr_id,tb_addr,tb_addr_id_num  from {0}  with(nolock) where tb_addr_id_num>{1}+1000+{2} and tb_addr_id_num<={1}+2000+{2}-1", tablename, minc.ToString(), addset);
                    DSToT2 = SimpleDataHelper.Query(SimpleDataHelper.MSConnectionString, sql2);

                    string sql3 = string.Format("select tb_addr_id,tb_addr,tb_addr_id_num  from {0}  with(nolock) where tb_addr_id_num>{1}+2000+{2} and tb_addr_id_num<={1}+3000+{2}-1", tablename, minc.ToString(), addset);
                    DSToT3 = SimpleDataHelper.Query(SimpleDataHelper.MSConnectionString, sql3);

                    string sql4 = string.Format("select tb_addr_id,tb_addr,tb_addr_id_num  from {0}  with(nolock) where tb_addr_id_num>{1}+3000+{2} and tb_addr_id_num<={1}+4000+{2}-1", tablename, minc.ToString(), addset);
                    DSToT4 = SimpleDataHelper.Query(SimpleDataHelper.MSConnectionString, sql4);

                    string sql5 = string.Format("select tb_addr_id,tb_addr,tb_addr_id_num  from {0}  with(nolock) where tb_addr_id_num>{1}+4000+{2} and tb_addr_id_num<={1}+5000+{2}-1", tablename, minc.ToString(), addset);
                    DSToT5 = SimpleDataHelper.Query(SimpleDataHelper.MSConnectionString, sql5);


                    //Console.WriteLine("===== 异步回调 AsyncInvoke =====");


                    var t1 = Task.Factory.StartNew(() => AddressMatch.Program.AddrMatch("bkVoYqmhhPWZavSf59pYsWgo1kvPGDXh", "上海", DSToT1));

                    var t2 = Task.Factory.StartNew(() => AddressMatch.Program.AddrMatch("G9cddRpBtdh8gtVMT6gSnS1b9reuCKHs", "上海", DSToT2));

                    var t3 = Task.Factory.StartNew(() => AddressMatch.Program.AddrMatch("HdA7n8YuTHzfdLLYgwWdf4LaZWRgcmeG", "上海", DSToT3));
                    var t4 = Task.Factory.StartNew(() => AddressMatch.Program.AddrMatch("Y5DStlEzvc0a228OeecY8I3Dqm1CzoZb", "上海", DSToT4));
                    var t5 = Task.Factory.StartNew(() => AddressMatch.Program.AddrMatch("bRbMPYGGgFPNqmxy80rDt3Gh", "上海", DSToT5));

                    Console.WriteLine("等待中。。。");

                    Task.WaitAll(t1, t2, t3, t4, t5);
                    //Task.WaitAll(t1);


                    DSFromT = t1.Result;

                    DSFromT.Merge(t2.Result);

                    DSFromT.Merge(t3.Result);

                    DSFromT.Merge(t4.Result);

                    DSFromT.Merge(t5.Result);

                    string tbaddridnumSet = "";

                    foreach (DataRow dr in DSFromT.Tables[0].Rows)
                    {
                        tbaddridnumSet += dr[1].ToString() + ",";

                    }

                    string sqldelete = string.Format("delete {0} where tb_addr_id_num in (" + tbaddridnumSet.Substring(0, tbaddridnumSet.Length - 1) + ")", tablename);

                    SimpleDataHelper.Excsql(SimpleDataHelper.MSConnectionString, sqldelete);

                    SimpleDataHelper.SqlBCP(SimpleDataHelper.MSConnectionString, DSFromT.Tables[0], tablename);
                }
                catch(Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }


            }
            Console.WriteLine("Job's Done! Press Any Key To Continue...");

            Console.ReadKey();


        }




    }
}
