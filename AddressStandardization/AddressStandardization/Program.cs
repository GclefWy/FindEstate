using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.Sql;
using System.Data;
using System.Data.SqlClient;
using System.Configuration;
using System.Text.RegularExpressions;

namespace AddressStandardization
{
    class Program
    {
        static string getAppSettingsByIndex(string prefix, int index)
        {
            if (ConfigurationManager.AppSettings[prefix + index.ToString()]==null)
            {
                return null;
            } else
            {
                return ConfigurationManager.AppSettings[prefix + index.ToString()].ToString();
            }
        }
        static void Main(string[] args)
        {
            string SConnDestination = ConfigurationManager.ConnectionStrings["SourceDBConn"].ConnectionString;
            string SUserID = ConfigurationManager.AppSettings["SUserID"].ToString();
            string SPSWD = ConfigurationManager.AppSettings["SPassword"].ToString();

            string TConnDestination = ConfigurationManager.ConnectionStrings["TargetDBConn"].ConnectionString;
            string TUserID = ConfigurationManager.AppSettings["TUserID"].ToString();
            string TPSWD = ConfigurationManager.AppSettings["TPassword"].ToString();

            string City = ConfigurationManager.AppSettings["City"].ToString();

            using (SqlConnection conn = new SqlConnection())
            {
                conn.ConnectionString = SConnDestination;
                conn.Open();

                using (SqlConnection conn2 = new SqlConnection())
                {
                    conn2.ConnectionString = TConnDestination;
                    conn2.Open();

                    using (SqlCommand command = conn.CreateCommand())
                    {
                        command.CommandText = "select id,contact_address from TB_MC_CUSTOMER_GFZ where id>5512294 and live_city='" + City + "'";

                        using (SqlDataAdapter adp = new SqlDataAdapter(command))
                        {
                            DataSet ds = new DataSet();
                            adp.Fill(ds);

                            for (int i = 0; i < ds.Tables[0].Rows.Count; i++)
                            {
                                string customerID = ds.Tables[0].Rows[i][0].ToString();
                                string addrS = ds.Tables[0].Rows[i][1].ToString();

                                Console.WriteLine(customerID+" : "+addrS);

                                string addrT = addrS;
                                int index = 1;
                                while (getAppSettingsByIndex("Search_",index) != null)
                                {
                                    addrT = Regex.Match(addrT, getAppSettingsByIndex("Search_", index)).Success ? Regex.Match(addrT, getAppSettingsByIndex("Search_", index)).Result(getAppSettingsByIndex("Replace_", index)) : addrT;

                                    index++;
                                }

                                if (addrT.Trim().Length>0)
                                {
                                    Guid addrID = Guid.NewGuid();
                                    using (SqlCommand command2 = conn2.CreateCommand())
                                    {
                                        command2.CommandText = "select tb_addr_id from TB_ADDR where tb_addr='" + addrT + "'";

                                        using (SqlDataAdapter adp2 = new SqlDataAdapter(command2))
                                        {
                                            DataSet ds2 = new DataSet();
                                            adp2.Fill(ds2);

                                            if (ds2.Tables[0].Rows.Count > 0)
                                            {
                                                addrID = new Guid(ds2.Tables[0].Rows[0][0].ToString());
                                            }
                                            else
                                            {
                                                string insertSQL2 = "insert into TB_ADDR (tb_addr_id, tb_addr) values('" + addrID + "','" + addrT + "')";
                                                command2.CommandText = insertSQL2;
                                                command2.ExecuteNonQuery();

                                            }

                                        }

                                    }

                                    Console.WriteLine(addrID + " : " + addrT);

                                    string insertSQL1 = "insert into TB_CUSTOMER_ADDR (customer_id, addr_id) values(" + customerID + ",'" + addrID + "')";

                                    using (SqlCommand command2 = conn2.CreateCommand())
                                    {
                                        command2.CommandText = insertSQL1;
                                        command2.ExecuteNonQuery();
                                    }

                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
