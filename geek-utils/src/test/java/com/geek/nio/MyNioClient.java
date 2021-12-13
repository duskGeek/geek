package com.geek.nio;


import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Date;
import java.util.Iterator;
import java.util.Scanner;

public class MyNioClient {

    SocketChannel socketChannel;
    ByteBuffer byteBuffer;
    Selector selector;

    public void connectServer() throws IOException {
        socketChannel = SocketChannel.open();
        socketChannel.configureBlocking(false);
        byteBuffer=ByteBuffer.allocate(1024);
        byteBuffer.order(ByteOrder.BIG_ENDIAN);
        selector = Selector.open();
        socketChannel.register(selector, SelectionKey.OP_CONNECT);
        socketChannel.connect(new InetSocketAddress("localhost",18801));
        recvie();
    }

    public void recvie() throws IOException {
        while (true){

            if(selector.select()>0){
                Iterator<SelectionKey> iterator=selector.selectedKeys().iterator();

                while (iterator.hasNext()){
                    SelectionKey selectionKey=iterator.next();
                    if (selectionKey.isConnectable()) {
                      SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
                      socketChannel.finishConnect();
                      socketChannel.register(selector,SelectionKey.OP_WRITE,ByteBuffer.allocateDirect(4096));
                      selectionKey.interestOps(SelectionKey.OP_WRITE);
                      System.out.println("channel Connectable"+new Date().getTime());
                    }
                    if(selectionKey.isReadable()){
                        SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
                        ByteBuffer buffer = ByteBuffer.allocate(128);
                        socketChannel.read(buffer);
                        System.out.println("channel revice: "+new String(buffer.array()));
//                        byteBuffer.clear();
//                        int count=0;
//                        while ((count=socketChannel.read(byteBuffer))>0){
//                            byteBuffer.flip();
//                            if(byteBuffer.hasRemaining()){
//                                System.out.println("channel revice: "+byteBuffer.get());
//                            }
//                            byteBuffer.clear();
//                        }
                        selectionKey.interestOps(SelectionKey.OP_WRITE);
                        System.out.println("channel Readable"+new Date().getTime());
                    }else if (selectionKey.isWritable()) {
                        SocketChannel clientChannel = (SocketChannel) selectionKey.channel();
                        String str = "qiwoo mobile";
                        ByteBuffer buffer = ByteBuffer.wrap(str.getBytes());
                        clientChannel.write(buffer);
                        selectionKey.interestOps(SelectionKey.OP_READ);
                        System.out.println("向服务端发送数据" + new String(buffer.array()));
                    }

                    iterator.remove();
                }

                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }


        }

    }

    public static void main(String[] args) {
        try {
            new MyNioClient().connectServer();
        } catch (IOException e) {
            e.printStackTrace();
        }

//        Socket socket= new Socket();
//        try {
//            socket.connect(new InetSocketAddress("localhost",18801));
//            while (true){
//                Scanner scanner=  new Scanner(System.in);
//                String msg=scanner.next();
//                socket.getOutputStream().write(msg.getBytes());
//            }
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }
}
