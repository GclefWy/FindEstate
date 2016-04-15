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
using System.Web;
using System.Net;
using System.IO;
using Newtonsoft.Json.Linq;
using System.Threading;

namespace AddressMatch
{
    public class Program
    {
        static string getHttp(string url, string queryString)
        {
            //string queryString = "?";

            //foreach (string key in httpContext.Request.QueryString.AllKeys)
            //{
            //queryString += key + "=" + httpContext.Request.QueryString[key] + "&";
            //}

            //queryString = queryString.Substring(0, queryString.Length - 1);

            HttpWebRequest httpWebRequest = (HttpWebRequest)WebRequest.Create(url + queryString);
            //Console.WriteLine(httpWebRequest.RequestUri);

            httpWebRequest.ContentType = "application/json";
            httpWebRequest.Method = "GET";
            httpWebRequest.Timeout = 20000;

            //byte[] btBodys = Encoding.UTF8.GetBytes(body);
            //httpWebRequest.ContentLength = btBodys.Length;
            //httpWebRequest.GetRequestStream().Write(btBodys, 0, btBodys.Length);

            string responseContent = "";
            HttpWebResponse httpWebResponse;
            StreamReader streamReader;
            try
            {
                httpWebResponse = (HttpWebResponse)httpWebRequest.GetResponse();
                streamReader = new StreamReader(httpWebResponse.GetResponseStream());
                responseContent = streamReader.ReadToEnd();
                httpWebResponse.Close();
                streamReader.Close();
            }
            catch
            {
                responseContent = "";

            }

            return responseContent;
        }

        static string getAddrRoadName(string addr)
        {
            return Regex.Match(addr, @"(\w*市)*(\w*区)*(?'A'\w*?)(\d+|$)").Success ? Regex.Match(addr, @"(\w*市)*(\w*区)*(?'A'\w*?)(\d+|$)").Result(@"${A}") : "";
        }

        static string getAddrNum(string addr)
        {
            return Regex.Match(addr, @"(?'A'\d+)").Success ? Regex.Match(addr, @"(?'A'\d+)").Result(@"${A}") : "";
        }

        public static DataSet AddrMatch(string ak, string city, DataSet ds)
        {
            DataSet dst = new DataSet();
            DataTable dt = new DataTable();
            dst.Tables.Add(dt);
            dt.Columns.Add("tb_addr_id", typeof(Guid));
            dt.Columns.Add("tb_addr_id_num", typeof(Int32));
            dt.Columns.Add("tb_addr", typeof(string));
            dt.Columns.Add("tb_geocoderAPI", typeof(string));
            dt.Columns.Add("tb_lat", typeof(float));
            dt.Columns.Add("tb_lng", typeof(float));
            dt.Columns.Add("tb_placeAPI", typeof(string));
            dt.Columns.Add("tb_unit", typeof(string));
            dt.Columns.Add("tb_unit_confidence", typeof(Int32));
            dt.Columns.Add("tb_selectAPI", typeof(string));
            dt.Columns.Add("tb_estate_id", typeof(string));
            dt.Columns.Add("tb_estate_id_confidence", typeof(Int32));

            for (int i = 0; i < ds.Tables[0].Rows.Count; i++)
            {
                try
                {
                    string updateSQL = "";
                    DataRow new_row = dt.NewRow();

                    string addrID = ds.Tables[0].Rows[i][0].ToString();
                    string addrIDNum = ds.Tables[0].Rows[i][2].ToString();
                    string addr = ds.Tables[0].Rows[i][1].ToString();

                    new_row["tb_addr_id"] = addrID;
                    new_row["tb_addr_id_num"] = addrIDNum;
                    new_row["tb_addr"] = addr;

                    Console.WriteLine("");
                    Console.WriteLine(addrID + " : " + addr);

                    string geocoderURL = @"http://api.map.baidu.com/geocoder/v2/";
                    string geocoderQueryString = @"?address=" + addr + "&output=json&ak=" + ak;

                    string rtn = getHttp(geocoderURL, geocoderQueryString);

                    if (rtn.Length > 0)
                    {

                        JObject jo = JObject.Parse(rtn);

                        if (((int)jo["status"]) == 0)
                        {

                            string lng = (string)jo["result"]["location"]["lng"];
                            string lat = (string)jo["result"]["location"]["lat"];

                            Console.WriteLine("lng :" + lng + " , lat : " + lat);

                            int pagesize = 20;
                            string placeURL = @"http://api.map.baidu.com/place/v2/search";
                            string placeQueryString = @"?output=json&query=%E5%B0%8F%E5%8C%BA&page_size=" + pagesize.ToString() + "&page_num=0&scope=1&location=" + lat + "," + lng + "&radius=500&ak=" + ak;

                            string rtn2 = getHttp(placeURL, placeQueryString);

                            if (rtn2.Length > 0)
                            {
                                JObject jo2 = JObject.Parse(rtn2);

                                if (((int)jo2["status"]) == 0)
                                {

                                    int total = (int)jo2["total"];
                                    string unit = "";
                                    int unitConfidence = 0;
                                    for (int k = 0; k < (total < pagesize ? total : pagesize); k++)
                                    {
                                        if (k == 0)
                                        {
                                            unit = (string)jo2["results"][k]["name"];
                                            unitConfidence = 10;
                                        }
                                        if ((Regex.Match((string)jo2["results"][k]["address"], getAddrRoadName(addr)).Success) && (Regex.Match((string)jo2["results"][k]["address"], getAddrNum(addr)).Success))
                                        {
                                            unit = (string)jo2["results"][k]["name"];
                                            unitConfidence = 80;
                                            break;
                                        }
                                    }

                                    updateSQL += "tb_geocoderAPI = '" + rtn + "'";
                                    new_row["tb_geocoderAPI"] = rtn;
                                    updateSQL += ",tb_lng=" + lng + ",tb_lat=" + lat;
                                    new_row["tb_lng"] = lng;
                                    new_row["tb_lat"] = lat;
                                    updateSQL += ",tb_placeAPI='" + rtn2 + "'";
                                    new_row["tb_placeAPI"] = rtn2;

                                    if (unit.Length > 0)
                                    {

                                        Console.WriteLine(unit);

                                        string selectURL = "http://172.28.70.31:8080/two/collection1/select";
                                        string selecttQueryString = "?q=MultiName:" + unit + "&wt=json&indent=true&rows=5&fq=CityName:"+ city;

                                        string rtn3 = getHttp(selectURL, selecttQueryString);

                                        if (rtn3.Length > 0)
                                        {

                                            updateSQL += ",tb_unit='" + unit + "',tb_unit_confidence=" + unitConfidence.ToString();
                                            new_row["tb_unit"] = unit;
                                            new_row["tb_unit_confidence"] = unitConfidence;
                                            updateSQL += ",tb_selectAPI='" + rtn3 + "'";
                                            new_row["tb_selectAPI"] = rtn3;

                                            JObject jo3 = JObject.Parse(rtn3);

                                            int rows = (int)jo3["responseHeader"]["params"]["rows"];
                                            string EstateID = "";
                                            int EstateIDConfidence = 0;
                                            try
                                            {
                                                for (int j = 0; j < rows; j++)
                                                {
                                                    if (j == 0)
                                                    {
                                                        EstateID = (string)jo3["response"]["docs"][j]["EstateID"];
                                                        EstateIDConfidence = 10;
                                                    }
                                                    if ((Regex.Match((string)jo3["response"]["docs"][j]["EstateName"], unit).Success) || ((Regex.Match((string)jo3["response"]["docs"][j]["Address"], getAddrRoadName(addr)).Success) && (Regex.Match((string)jo3["response"]["docs"][j]["Address"], getAddrNum(addr)).Success)))
                                                    {
                                                        EstateID = (string)jo3["response"]["docs"][j]["EstateID"];
                                                        EstateIDConfidence = 80;


                                                        break;
                                                    }

                                                }

                                                if (EstateID.Length > 0)
                                                {
                                                    updateSQL += ",tb_estate_id='" + EstateID + "', tb_estate_id_confidence=" + EstateIDConfidence.ToString();
                                                    new_row["tb_estate_id"] = EstateID;
                                                    new_row["tb_estate_id_confidence"] = EstateIDConfidence;

                                                    Console.WriteLine(EstateID);

                                                }

                                            }
                                            catch
                                            {

                                            }
                                        }
                                    }

                                }
                                else
                                {
                                    Console.WriteLine("get placeURL fail : " + rtn2);
                                    if (((int)jo["status"]) == 302)
                                    {
                                        Thread.Sleep(30 * 60 * 1000);
                                    }

                                }
                            }
                        }
                        else
                        {
                            Console.WriteLine("get geocoderURL fail : " + rtn);
                            if (((int)jo["status"]) == 302)
                            {
                                Thread.Sleep(30 * 60 * 1000);
                            }
                        }
                    }

                    
                        dt.Rows.Add(new_row);
                    

                    

                }
                catch(Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
            }

            return dst;
        }

        static void Main(string[] args)
        {
            string SConnDestination = ConfigurationManager.ConnectionStrings["SourceDBConn"].ConnectionString;
            string SUserID = ConfigurationManager.AppSettings["SUserID"].ToString();
            string SPSWD = ConfigurationManager.AppSettings["SPassword"].ToString();
            string SRangeItem = ConfigurationManager.AppSettings["SRangeItem"].ToString();
            string SRangeMin = ConfigurationManager.AppSettings["SRangeMin"].ToString();
            string SRangeMax = ConfigurationManager.AppSettings["SRangeMax"].ToString();
            string SRange = "(" + SRangeMin + "<=" + SRangeItem + " and " + SRangeItem + "<=" + SRangeMax + ")";

            string TConnDestination = ConfigurationManager.ConnectionStrings["TargetDBConn"].ConnectionString;
            string TUserID = ConfigurationManager.AppSettings["TUserID"].ToString();
            string TPSWD = ConfigurationManager.AppSettings["TPassword"].ToString();

            string ak = ConfigurationManager.AppSettings["ak"].ToString();
            Random ra = new Random();

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
                        command.CommandText = "select top 10 tb_addr_id,tb_addr from TB_ADDR  with(nolock) ";// where tb_geocoderAPI is null ";//and " + SRange;
                        //command.CommandText = "select tb_addr_id,tb_addr from TB_ADDR with(nolock) where tb_addr_id = 'A124F222-6293-4572-AD3C-765E207C867C'";
                        
                        using (SqlDataAdapter adp = new SqlDataAdapter(command))
                        {
                            DataSet ds = new DataSet();
                            adp.Fill(ds);
                            Console.WriteLine(AddrMatch(ak, "上海", ds));

                            //for (int i = 0; i < ds.Tables[0].Rows.Count; i++)
                            //{
                            //    try
                            //    {
                            //        string updateSQL = "";

                            //        string addrID = ds.Tables[0].Rows[i][0].ToString();
                            //        string addr = ds.Tables[0].Rows[i][1].ToString();

                            //        Console.WriteLine("");
                            //        Console.WriteLine(addrID + " : " + addr);

                            //        string geocoderURL = @"http://api.map.baidu.com/geocoder/v2/";
                            //        string geocoderQueryString = @"?address=" + addr + "&output=json&ak=" + ak;

                            //        string rtn = getHttp(geocoderURL, geocoderQueryString);

                            //        if (rtn.Length > 0)
                            //        {

                            //            JObject jo = JObject.Parse(rtn);

                            //            if (((int)jo["status"]) == 0)
                            //            {

                            //                string lng = (string)jo["result"]["location"]["lng"];
                            //                string lat = (string)jo["result"]["location"]["lat"];

                            //                Console.WriteLine("lng :" + lng + " , lat : " + lat);

                            //                int pagesize = 20;
                            //                string placeURL = @"http://api.map.baidu.com/place/v2/search";
                            //                string placeQueryString = @"?output=json&query=%E5%B0%8F%E5%8C%BA&page_size=" + pagesize.ToString() + "&page_num=0&scope=1&location=" + lat + "," + lng + "&radius=500&ak=" + ak;

                            //                string rtn2 = getHttp(placeURL, placeQueryString);

                            //                if (rtn2.Length > 0)
                            //                {
                            //                    JObject jo2 = JObject.Parse(rtn2);

                            //                    if (((int)jo2["status"]) == 0)
                            //                    {

                            //                        int total = (int)jo2["total"];
                            //                        string unit = "";
                            //                        int unitConfidence = 0;
                            //                        for (int k = 0; k < (total < pagesize ? total : pagesize); k++)
                            //                        {
                            //                            if (k == 0)
                            //                            {
                            //                                unit = (string)jo2["results"][k]["name"];
                            //                                unitConfidence = 10;
                            //                            }
                            //                            if ((Regex.Match((string)jo2["results"][k]["address"], getAddrRoadName(addr)).Success) && (Regex.Match((string)jo2["results"][k]["address"], getAddrNum(addr)).Success))
                            //                            {
                            //                                unit = (string)jo2["results"][k]["name"];
                            //                                unitConfidence = 80;
                            //                                break;
                            //                            }
                            //                        }

                            //                        updateSQL += "tb_geocoderAPI = '" + rtn + "'";
                            //                        updateSQL += ",tb_lng=" + lng + ",tb_lat=" + lat;
                            //                        updateSQL += ",tb_placeAPI='" + rtn2 + "'";

                            //                        //using (SqlCommand command2 = conn2.CreateCommand())
                            //                        //{
                            //                        //    command2.CommandText = "update TB_ADDR with(rowlock) set tb_geocoderAPI='" + rtn + "' where tb_addr_id='" + addrID + "'";
                            //                        //    command2.ExecuteNonQuery();

                            //                        //    command2.CommandText = "update TB_ADDR with(rowlock) set tb_lng=" + lng + ",tb_lat=" + lat + " where tb_addr_id='" + addrID + "'";
                            //                        //    command2.ExecuteNonQuery();

                            //                        //    command2.CommandText = "update TB_ADDR with(rowlock) set tb_placeAPI='" + rtn2 + "' where tb_addr_id='" + addrID + "'";
                            //                        //    command2.ExecuteNonQuery();
                            //                        //}

                            //                        if (unit.Length > 0)
                            //                        {

                            //                            Console.WriteLine(unit);

                            //                            string selectURL = "http://172.28.70.31:8080/two/collection1/select";
                            //                            string selecttQueryString = "?q=MultiName:" + unit + "&wt=json&indent=true&rows=5&fq=CityName:上海";

                            //                            string rtn3 = getHttp(selectURL, selecttQueryString);

                            //                            if (rtn3.Length > 0)
                            //                            {

                            //                                updateSQL += ",tb_unit='" + unit + "',tb_unit_confidence=" + unitConfidence.ToString();
                            //                                updateSQL += ",tb_selectAPI='" + rtn3 + "'";

                            //                                //using (SqlCommand command2 = conn2.CreateCommand())
                            //                                //{
                            //                                //    command2.CommandText = "update TB_ADDR with(rowlock) set tb_unit='" + unit + "',tb_unit_confidence=" + unitConfidence.ToString() + " where tb_addr_id='" + addrID + "'";
                            //                                //    command2.ExecuteNonQuery();

                            //                                //    command2.CommandText = "update TB_ADDR with(rowlock) set tb_selectAPI='" + rtn3 + "' where tb_addr_id='" + addrID + "'";
                            //                                //    command2.ExecuteNonQuery();

                            //                                //}


                            //                                JObject jo3 = JObject.Parse(rtn3);

                            //                                int rows = (int)jo3["responseHeader"]["params"]["rows"];
                            //                                string EstateID = "";
                            //                                int EstateIDConfidence = 0;
                            //                                try
                            //                                {
                            //                                    for (int j = 0; j < rows; j++)
                            //                                    {
                            //                                        if (j == 0)
                            //                                        {
                            //                                            EstateID = (string)jo3["response"]["docs"][j]["EstateID"];
                            //                                            EstateIDConfidence = 10;
                            //                                        }
                            //                                        if ((Regex.Match((string)jo3["response"]["docs"][j]["EstateName"], unit).Success) || ((Regex.Match((string)jo3["response"]["docs"][j]["Address"], getAddrRoadName(addr)).Success) && (Regex.Match((string)jo3["response"]["docs"][j]["Address"], getAddrNum(addr)).Success)))
                            //                                        {
                            //                                            EstateID = (string)jo3["response"]["docs"][j]["EstateID"];
                            //                                            EstateIDConfidence = 80;


                            //                                            break;
                            //                                        }

                            //                                    }

                            //                                    if (EstateID.Length > 0)
                            //                                    {
                            //                                        updateSQL += ",tb_estate_id='" + EstateID + "', tb_estate_id_confidence=" + EstateIDConfidence.ToString();

                            //                                        //using (SqlCommand command2 = conn2.CreateCommand())
                            //                                        //{
                            //                                        //    command2.CommandText = "update TB_ADDR with(rowlock) set tb_estate_id='" + EstateID + "', tb_estate_id_confidence=" + EstateIDConfidence.ToString() + " where tb_addr_id='" + addrID + "'";
                            //                                        //    command2.ExecuteNonQuery();

                            //                                        //}

                            //                                        Console.WriteLine(EstateID);

                            //                                    }

                            //                                }
                            //                                catch
                            //                                {

                            //                                }
                            //                            }
                            //                        }

                            //                    }
                            //                    else
                            //                    {
                            //                        Console.WriteLine("get placeURL fail : " + rtn2);
                            //                        if (((int)jo["status"]) == 302)
                            //                        {
                            //                            Thread.Sleep(30 * 60 * 1000);
                            //                        }

                            //                    }
                            //                }
                            //            }
                            //            else
                            //            {
                            //                Console.WriteLine("get geocoderURL fail : " + rtn);
                            //                if (((int)jo["status"]) == 302)
                            //                {
                            //                    Thread.Sleep(30 * 60 * 1000);
                            //                }
                            //            }
                            //        }

                            //        if (updateSQL.Length>0)
                            //        {
                            //            using (SqlCommand command2 = conn2.CreateCommand())
                            //            {
                            //                command2.CommandText = "update TB_ADDR with(rowlock) set "+updateSQL+" where tb_addr_id='" + addrID + "'";

                            //                Boolean sqlExecSuccess = false;
                            //                int sqlExecRetryCount = 5;

                            //                while (sqlExecRetryCount>0 && (!sqlExecSuccess))
                            //                {
                            //                    try
                            //                    {
                            //                        Console.WriteLine("update SQL start ...");
                            //                        command2.ExecuteNonQuery();
                            //                        sqlExecSuccess = true;
                            //                        Console.WriteLine("update SQL end");
                            //                    }
                            //                    catch
                            //                    {
                            //                        Console.WriteLine("update SQL error");
                            //                        Console.WriteLine(command2.CommandText);
                            //                        Thread.Sleep(ra.Next(10)*200);
                            //                        Console.WriteLine("update SQL retry ("+ sqlExecRetryCount + ")");
                            //                    }
                            //                    finally
                            //                    {
                            //                        sqlExecRetryCount--;
                            //                    }

                            //                }

                            //            }

                                //    }
                                //}
                                //catch
                                //{

                                //}
                            //}
                        }
                    }
                }
            }
        }
    }
}
