package com.demo.cn.com.socket.cn;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NIOSocketServer {

    private static ExecutorService executorService= Executors.newFixedThreadPool(10);
    private static class HandleMsg implements Runnable{
        Socket clinet;

        public HandleMsg(Socket socket){this.clinet=socket;}
        @Override
        public void run() {
            BufferedReader bufferedReader=null;
            PrintWriter pw=null;

            try {
                bufferedReader=new BufferedReader(new InputStreamReader(clinet.getInputStream()));
                pw=new PrintWriter(clinet.getOutputStream(),true);
                String inputLine=null;
                long a = System.currentTimeMillis();
                while ((inputLine = bufferedReader.readLine())!=null){
                    pw.println("message form server :"+inputLine);
                }
                long b = System.currentTimeMillis();
                System.out.println("此线程花费了："+(b-a)+"秒！");
            } catch (IOException e) {
                e.printStackTrace();
            }finally {
                try {
                    bufferedReader.close();
                    pw.close();
                    clinet.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }
    }

    public static void main(String []args){
        try {
            ServerSocket serverSocket=new ServerSocket(9000,5, InetAddress.getByName("172.21.209.106"));
            Socket socket=null;
            while (true){
                socket=serverSocket.accept();
                System.out.println(socket.getRemoteSocketAddress()+"地址的客户端连接成功!");
                executorService.submit(new HandleMsg(socket));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
