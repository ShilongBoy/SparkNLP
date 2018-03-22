package com.demo.cn.com.socket.cn;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;

public class NIOClientServer {

    public static void main(String []args) throws IOException {
        Socket socket=null;
        BufferedReader bufferedReader=null;
        PrintWriter printWriter=null;
        socket=new Socket();
        try {
            socket.connect(new InetSocketAddress("172.21.209.106",9000));
            printWriter = new PrintWriter(socket.getOutputStream(),true);
            printWriter.println("hello");
            printWriter.flush();

            bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));            //读取服务器返回的信息并进行输出
            System.out.println("来自服务器的信息是："+bufferedReader.readLine());
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
                printWriter.close();
                bufferedReader.close();
                socket.close();

        }
    }

}
