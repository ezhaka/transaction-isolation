package seal.happy.isolation

import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.sql.Connection
import java.time.Duration
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread

class TransactionIsolationTests {
    @BeforeEach
    fun setup() {
        connect()

        transaction {
            KeyValue.deleteAll()
        }
    }

    @Test
    fun `dirty read, read uncommitted`() {
        assertTrue(tryPerformDirtyRead(Connection.TRANSACTION_READ_UNCOMMITTED))
    }

    @Test
    fun `dirty read, read committed`() {
        assertFalse(tryPerformDirtyRead(Connection.TRANSACTION_READ_COMMITTED))
    }

    @Test
    fun `non-repeatable read, read committed`() {
        assertTrue(tryPerformNonRepeatableRead(Connection.TRANSACTION_READ_COMMITTED))
    }

    @Test
    fun `non-repeatable read, repeatable read`() {
        assertFalse(tryPerformNonRepeatableRead(Connection.TRANSACTION_REPEATABLE_READ))
    }

    @Test
    fun `write skew, repeatable read`() {
        assertTrue(tryPerformWriteSkew(Connection.TRANSACTION_REPEATABLE_READ))
    }

    @Test
    @Disabled
    fun `write skew, serializable`() {
        assertTimeoutPreemptively(Duration.ofSeconds(5)) {
            tryPerformWriteSkew(Connection.TRANSACTION_SERIALIZABLE)
        }
    }

    @Test
    fun `concurrent increment, read uncommitted`() {
        val result = performConcurrentIncrement(Connection.TRANSACTION_READ_UNCOMMITTED, iterations = 1000)
        assertNotEquals(1000, result)
    }

    @Test
    fun `concurrent increment, read committed`() {
        val result = performConcurrentIncrement(Connection.TRANSACTION_READ_COMMITTED, iterations = 1000)
        assertNotEquals(1000, result)
    }

    @Test
    fun `concurrent increment, repeatable read`() {
        val result = performConcurrentIncrement(Connection.TRANSACTION_REPEATABLE_READ, iterations = 1000)
        assertNotEquals(1000, result)
    }

    @Test
    fun `concurrent increment, serializable`() {
        val result = performConcurrentIncrement(Connection.TRANSACTION_SERIALIZABLE, iterations = 1000)
        assertEquals(1000, result)
    }

    private fun tryPerformDirtyRead(transactionIsolation: Int): Boolean {
        val pinkValue = AtomicInteger()
        val aliceToBob = LinkedBlockingDeque<Unit>(1)
        val bobToAlice = LinkedBlockingDeque<Unit>(1)

        val committedValue = 1
        val uncommittedValue = 2

        transaction {
            KeyValue.insert {
                it[key] = "pink"
                it[value] = committedValue
            }
        }

        val aliceThread = thread {
            transaction(transactionIsolation) {
                seal.happy.isolation.KeyValue.update(where = { KeyValue.key eq "pink" }) {
                    it[value] = uncommittedValue
                }

                aliceToBob.add(Unit)
                bobToAlice.take()
            }
        }

        val bobThread = thread {
            transaction(transactionIsolation) {
                aliceToBob.take()
                pinkValue.set(seal.happy.isolation.KeyValue.select { KeyValue.key eq "pink" }.first()[KeyValue.value])
                bobToAlice.add(Unit)
            }
        }

        aliceThread.join()
        bobThread.join()

        return pinkValue.get() == uncommittedValue
    }

    private fun tryPerformNonRepeatableRead(transactionIsolation: Int): Boolean {
        val aliceToBob = LinkedBlockingDeque<Unit>(1)
        val bobToAlice = LinkedBlockingDeque<Unit>(1)

        transaction {
            KeyValue.insert {
                it[key] = "teal"
                it[value] = 1
            }
        }

        val aliceThread = thread {
            transaction(transactionIsolation) {
                bobToAlice.take()

                seal.happy.isolation.KeyValue.update(where = { KeyValue.key eq "teal" }) {
                    it[value] = 2
                }
            }

            aliceToBob.add(Unit)
        }

        val firstReadValue = AtomicInteger()
        val secondReadValue = AtomicInteger()

        val bobThread = thread {
            transaction(transactionIsolation) {
                firstReadValue.set(seal.happy.isolation.KeyValue.select { KeyValue.key eq "teal" }.first()[KeyValue.value])

                bobToAlice.add(Unit)
                aliceToBob.take()

                secondReadValue.set(seal.happy.isolation.KeyValue.select { KeyValue.key eq "teal" }.first()[KeyValue.value])
            }
        }

        aliceThread.join()
        bobThread.join()

        return firstReadValue.get() != secondReadValue.get()
    }

    private fun tryPerformWriteSkew(transactionIsolation: Int): Boolean {
        val aliceToBob = LinkedBlockingDeque<Unit>(1)
        val bobToAlice = LinkedBlockingDeque<Unit>(1)

        val aliceThread = thread {
            transaction(transactionIsolation) {
                bobToAlice.take()

                seal.happy.isolation.KeyValue.insert { it[key] = "pink" }
            }

            aliceToBob.add(Unit)
        }

        val bobThread = thread {
            transaction(transactionIsolation) {
                val pinkCount =
                    seal.happy.isolation.KeyValue.select { KeyValue.key eq "pink" }.count()

                bobToAlice.add(Unit)
                aliceToBob.take()

                if (pinkCount == 0) {
                    seal.happy.isolation.KeyValue.insert { it[key] = "pink" }
                }
            }
        }

        aliceThread.join()
        bobThread.join()

        val pinkCount = transaction { KeyValue.select { KeyValue.key eq "pink" }.count() }
        return pinkCount == 2
    }

    private fun performConcurrentIncrement(transactionIsolation: Int, iterations: Int = 1000): Int {
        transaction(transactionIsolation) {
            KeyValue.insert {
                it[key] = "orange"
                it[value] = 0
            }
        }

        val pool = Executors.newFixedThreadPool(Math.max(Runtime.getRuntime().availableProcessors(), 8))
        val completedTransactions = AtomicInteger()

        for (i in 1..iterations) {
            pool.submit {
                transaction(transactionIsolation, repetitionAttempts = iterations) {
                    val count = KeyValue.select { KeyValue.key eq "orange" }.first()[KeyValue.value]
                    KeyValue.update(where = { KeyValue.key eq "orange" }) { it[value] = count + 1 }
                }

                completedTransactions.incrementAndGet()
            }
        }

        pool.shutdown()
        pool.awaitTermination(1, TimeUnit.MINUTES)

        assertEquals(iterations, completedTransactions.get())

        return transaction(transactionIsolation) {
            KeyValue.select { KeyValue.key eq "orange" }.first()[KeyValue.value]
        }
    }
}
