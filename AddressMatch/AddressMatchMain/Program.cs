using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using AddressMatch;
using System.Data;
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

            DataSet DSToT1 = new DataSet();
            DataSet DSFromT1 = new DataSet();

            DataSet DSToT2 = new DataSet();
            DataSet DSFromT2 = new DataSet();

            string sql1 = "select tb_addr_id,tb_addr from TB_ADDR  with(nolock) where tb_addr_id_num<=10";
             DSToT1 = SimpleDataHelper.Query(SimpleDataHelper.MSConnectionString, sql1);

            string sql2 = "select tb_addr_id,tb_addr from TB_ADDR  with(nolock) where tb_addr_id_num>100 and tb_addr_id_num<=200";
            DSToT2 = SimpleDataHelper.Query(SimpleDataHelper.MSConnectionString, sql2);

            Console.WriteLine("===== 异步回调 AsyncInvoke =====");

            DSFromT1 = AddressMatch.Program.AddrMatch("bkVoYqmhhPWZavSf59pYsWgo1kvPGDXh","上海", DSToT1);

            //AddressMatchHandler AMhandler1 = new AddressMatchHandler(AddressMatch.Program.AddrMatch);
            //AddressMatchHandler AMhandler2 = new AddressMatchHandler(AddressMatch.Program.AddrMatch);

            //IAsyncResult: 异步操作接口(interface)
            //BeginInvoke: 委托(delegate)的一个异步方法的开始

            //IAsyncResult th1result = AMhandler.BeginInvoke("bkVoYqmhhPWZavSf59pYsWgo1kvPGDXh", DSToT1, new AsyncCallback(CallBackFun1), "AsycState:OK");
            //IAsyncResult th2result = AMhandler.BeginInvoke("bkVoYqmhhPWZavSf59pYsWgo1kvPGDXh", DSToT2, new AsyncCallback(CallBackFun2), "AsycState:OK");

            //IAsyncResult th1result = AMhandler1.BeginInvoke("bkVoYqmhhPWZavSf59pYsWgo1kvPGDXh", DSToT1, null, null);
            //IAsyncResult th2result = AMhandler2.BeginInvoke("bkVoYqmhhPWZavSf59pYsWgo1kvPGDXh", DSToT2, null, null);

            //DSFromT1 = AMhandler1.EndInvoke(th1result);
            //DSFromT2 = AMhandler2.EndInvoke(th2result);

            var t1 = Task.Factory.StartNew(() => AddressMatch.Program.AddrMatch("bkVoYqmhhPWZavSf59pYsWgo1kvPGDXh", "上海",DSToT1));
            var t2 = Task.Factory.StartNew(() => AddressMatch.Program.AddrMatch("bkVoYqmhhPWZavSf59pYsWgo1kvPGDXh", "上海",DSToT2));

            Task.WaitAll(t1, t2);


            DSFromT1 = t1.Result;

            DSFromT2 = t2.Result;

            Console.WriteLine("继续做别的事情。。。");
            //异步操作返回
            //Console.WriteLine(AMhandler.EndInvoke(th1result));
            Console.ReadKey();

            //Thread T1 = new Thread(DSFromT1 = AddressMatch.Program.AddrMatch("bkVoYqmhhPWZavSf59pYsWgo1kvPGDXh", DSToT1));
        }

        //private delegate DataSet AddressMatchHandler (string ak, DataSet ds);

        //线程回调函数
        //static void CallBackFun1(IAsyncResult result)
        //{
        //    //result 是“加法类.Add()方法”的返回值
        //    //AsyncResult 是IAsyncResult接口的一个实现类，空间：System.Runtime.Remoting.Messaging
        //    //AsyncDelegate 属性可以强制转换为用户定义的委托的实际类。
        //    AddressMatchHandler handler = (AddressMatchHandler)((AsyncResult)result).AsyncDelegate;
        //    //DSFromT1 = handler.EndInvoke(result);
        //    Console.WriteLine(result.AsyncState);
        //}

        //static void CallBackFun2(IAsyncResult result)
        //{
        //    //result 是“加法类.Add()方法”的返回值
        //    //AsyncResult 是IAsyncResult接口的一个实现类，空间：System.Runtime.Remoting.Messaging
        //    //AsyncDelegate 属性可以强制转换为用户定义的委托的实际类。
        //    AddressMatchHandler handler = (AddressMatchHandler)((AsyncResult)result).AsyncDelegate;
        //    //DSFromT1 = handler.EndInvoke(result);
        //    Console.WriteLine(result.AsyncState);
        //}


    }
}
