package exercises

import scala.concurrent.duration._
import scala.util.Random

import cats.Parallel
import cats.implicits._
import cats.effect.{ Concurrent, ExitCode, IO, IOApp, Sync, Timer }
import cats.effect.concurrent.{ MVar, Ref }

object workerpool {
  type Worker[F[_], A, B] = A => F[B]

  def mkWorker[F[_]: Timer: Sync](id: Int): F[Worker[F, Int, Int]] =
    Ref[F].of(0).map { counter =>
      def simulateWork: F[Unit] =
        Sync[F].delay(50 + Random.nextInt(450)).map(_.millis).flatMap(Timer[F].sleep)

      def report: F[Unit] =
        counter.get.flatMap(i => Sync[F].delay(println(s"Total processed by $id: $i")))

      x =>
        simulateWork >>
          counter.update(_ + 1) >>
          report >>
          Sync[F].pure(x + 1)
    }

  trait WorkerPool[F[_], A, B] {
    def exec(a: A): F[B]
  }

  object WorkerPool {
    def of[F[_]: Concurrent: Parallel, A, B](fs: List[Worker[F, A, B]]): F[WorkerPool[F, A, B]] =
      for {
        workers <- MVar.empty[F, Worker[F, A, B]]
        -       <- Concurrent[F].start(fs.parTraverse_(workers.put))
      } yield new WorkerPool[F, A, B] {
        override def exec(a: A): F[B] =
          for {
            worker <- workers.take
            b      <- worker(a)
            _      <- Concurrent[F].start(workers.put(worker))
          } yield b
      }
  }
}

object WorkerPoolApp extends IOApp {
  import workerpool._

  val testPool: IO[WorkerPool[IO, Int, Int]] =
    List
      .range(0, 10)
      .traverse(mkWorker[IO])
      .flatMap(WorkerPool.of[IO, Int, Int])

  def run(args: List[String]): IO[ExitCode] =
    testPool
      .flatMap { pool =>
        List
          .range(0, 100)
          .traverse(pool.exec)
      }
      .redeem(_ => ExitCode.Error, _ => ExitCode.Success)
}
