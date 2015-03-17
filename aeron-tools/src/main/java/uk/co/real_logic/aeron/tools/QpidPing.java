package uk.co.real_logic.aeron.tools;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.*;

import org.apache.qpid.client.AMQConnection;
import org.apache.qpid.client.AMQAnyDestination;
import org.apache.qpid.client.message.AbstractJMSMessage;

public class QpidPing implements PingImpl, MessageListener
{
  private Connection connection = null;
  private Session pingSession = null;
  private Session pongSession = null;
  private MessageProducer pingProducer = null;
  private Destination pingDestination = null;
  private Destination pongDestination = null;
  private String details = "localhost";
  private String username = "admin";
  private String password = "admin";
  private String selector = "selector";
  private String virtualPath = "";
  private String clientId = "perftest";
  private CountDownLatch pongedMessageLatch = null;
  private long rtt;

  public QpidPing()
  {

  }

  public void prepare()
  {
    try
    {
      connection = new AMQConnection("localhost", 5672, "admin", "password", "default", "/");
      //"amqp://guest:admin@test/?brokerlist='tcp://localhost:5672'");
      connection.start();

      pingSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      //pongSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      pingDestination = new AMQAnyDestination("ADDR:ping; {create: always}");
      //pongDestination = pongSession.createTopic("pong");

      //MessageConsumer pongConsumer = pongSession.createConsumer();

      pingProducer = pingSession.createProducer(null);
      pingProducer.setDisableMessageTimestamp(true);
      pingProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

      //pongConsumer.setMessageListener(this);
      pongedMessageLatch = new CountDownLatch(1);
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }

  public void connect()
  {
  }

  public long sendPingAndReceivePong(int msgLen)
  {
    try
    {
      System.out.println("Sending Message");
      BytesMessage m = pingSession.createBytesMessage();

      byte[] bytes = ByteBuffer.allocate(8).putLong(System.nanoTime()).array();
      m.writeBytes(bytes);
      pingProducer.send(pingDestination, m);

      System.out.println("Sent message");
      if (pongedMessageLatch.await(10, TimeUnit.SECONDS))
      {
        return rtt >> 1;
      }
      else
      {
        System.out.println("Timed out");
        return -1;
      }
    }
    catch (Exception e)
    {
      e.printStackTrace();
      return -1;
    }
  }

  public void shutdown()
  {

  }

  public void onMessage(Message msg)
  {
    try
    {
      System.out.println("Got Message");
      long pingts = Long.parseLong(((AbstractJMSMessage)msg).toBodyString());
      rtt = System.nanoTime() - pingts;
    }
    catch (Exception e)
    {
      rtt = -1;
    }

    pongedMessageLatch.countDown();
  }
}
