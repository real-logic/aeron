package uk.co.real_logic.aeron.tools;

import javax.jms.*;

import org.apache.qpid.client.AMQConnection;
import org.apache.qpid.client.AMQAnyDestination;
import org.apache.qpid.jms.Session;

public class QpidPong implements PongImpl, MessageListener
{
  private String destinationName = "ping";
  private Destination pingDestination = null;
  private Destination pongDestination = null;
  private MessageProducer pongProducer = null;
  private Session pingSession = null;
  private Session pongSession = null;
  private AMQConnection connection = null;
  private String details = "localhost";
  private String username = "admin";
  private String password = "admin";
  private String selector = "selector";
  private String virtualPath = "";
  private String clientId = "perftest";

  public QpidPong()
  {

  }

  public void prepare()
  {
    try
    {
      connection = new AMQConnection("amqp://admin:password@default/?brokerlist='tcp://localhost:5672'");
      connection.start();

      pingSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      //pongSession = (Session)connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      pingDestination = new AMQAnyDestination("ADDR:ping; {create: always}");
      //pongDestination = pongSession.createTopic("pong");

      MessageConsumer pingConsumer = pingSession.createConsumer(pingDestination);

      //pongProducer = pongSession.createProducer(null);
      //pongProducer.setDisableMessageTimestamp(true);
      //pongProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

      pingConsumer.setMessageListener(this);
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }

  public void run()
  {
  }

  public void shutdown()
  {

  }

  public void onMessage(Message message)
  {
    try
    {
      System.out.println("Got a message");
      //Destination responseDest = message.getJMSReplyTo();
      //message.setJMSCorrelationID(message.getJMSCorrelationID());
      //pongProducer.send(responseDest, message);
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }
}
