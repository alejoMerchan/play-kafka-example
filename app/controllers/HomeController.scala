package controllers

import javax.inject._

import akka.actor.ActorSystem
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.kafka.scaladsl.Consumer
import akka.stream.ActorMaterializer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import play.api.libs.EventSource
import play.api.mvc._

import scala.concurrent.ExecutionContext

/**
 * This controller creates an `Action` to handle HTTP requests to the
 * application's home page.
 */
@Singleton
class HomeController @Inject()(cc: ControllerComponents)(implicit ec:ExecutionContext) extends AbstractController(cc) {

  implicit val actorSystem = ActorSystem("KafkaStreamDemo")
  implicit val materializer = ActorMaterializer()

  /**
   * Create an Action to render an HTML page with a welcome message.
   * The configuration in the `routes` file means that this method
   * will be called when the application receives a `GET` request with
   * a path of `/`.
   */
  def index = Action {
    Ok(views.html.index("Your new application is ready."))
  }

  def kafkaStream = Action {
    Ok.chunked(kafkaStreamConsumer(kafkaConsumerSettings) via EventSource.flow)
  }


  def kafkaStreamConsumer(consumerSettings: ConsumerSettings[String,String]) =
    Consumer.plainSource(consumerSettings,Subscriptions.topics("eventos-prueba")).map(x => x.value())


  def kafkaConsumerSettings:ConsumerSettings[String, String] =
    ConsumerSettings(actorSystem,new StringDeserializer,new StringDeserializer)
      .withBootstrapServers("localhost:2181").withGroupId("ulp-ch03-3.3")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest")
}
