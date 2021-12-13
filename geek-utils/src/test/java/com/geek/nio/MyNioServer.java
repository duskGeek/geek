package com.geek.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Date;
import java.util.Iterator;
import java.util.Set;

public class MyNioServer {
    ServerSocketChannel channel;
    Selector selector;
    int port=18801;
    int bufferSize=1024;


    public MyNioServer() throws IOException {
        channel=ServerSocketChannel.open();
        channel.configureBlocking(false);
        channel.bind(new InetSocketAddress(port));
        selector=Selector.open();
        channel.register(selector, SelectionKey.OP_ACCEPT);
    }

    public void listener() throws IOException, InterruptedException {
        while (true){
            int n=selector.select();
            if(n==0){
                Thread.sleep(100);
                continue;
            }
            Set<SelectionKey> keys=selector.selectedKeys();
            Iterator<SelectionKey> iterator= keys.iterator();
            while (iterator.hasNext()){
                SelectionKey selectionKey=iterator.next();
                if(selectionKey.isAcceptable()){
                    SocketChannel channel=((ServerSocketChannel)selectionKey.channel()).accept();
                    channel.configureBlocking(false);
                    channel.register(selector,SelectionKey.OP_READ,ByteBuffer.allocateDirect(4096));
                    System.out.println("channel Acceptable"+new Date().getTime());
                }else if(selectionKey.isWritable()){
                    SocketChannel channel= (SocketChannel) selectionKey.channel();
                    replyClient(channel);
                    selectionKey.interestOps(SelectionKey.OP_READ);
                    System.out.println("channel Writable"+new Date().getTime());
                }else if(selectionKey.isReadable()){
                    SocketChannel socketChannel= (SocketChannel) selectionKey.channel();
                    readFromChannel(socketChannel);
                    selectionKey.interestOps(SelectionKey.OP_WRITE);
                    System.out.println("channel Readable"+new Date().getTime());
                }
                iterator.remove();
            }

        }

    }

    public void readFromChannel(SocketChannel socketChannel) throws IOException {
        ByteBuffer byteBuffer;
        byteBuffer=ByteBuffer.allocate(bufferSize);
        byteBuffer.order(ByteOrder.BIG_ENDIAN);
        int count=0;
        byteBuffer.clear();
        while ((count=socketChannel.read(byteBuffer))>0){
            System.out.println("接受客户端请求"+new String(byteBuffer.array()));
        }

    }


    public void replyClient(SocketChannel socketChannel) throws IOException {
//        ByteBuffer byteBuffer;
//        byteBuffer=ByteBuffer.allocate(bufferSize);
//        byteBuffer.order(ByteOrder.BIG_ENDIAN);

        socketChannel.write(ByteBuffer.wrap("hello client ~\r\n".getBytes()));
    }


    public static void main(String[] args) {
        try {
            new MyNioServer().listener();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
