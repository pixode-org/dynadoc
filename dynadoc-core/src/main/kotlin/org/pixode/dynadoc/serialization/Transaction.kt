package org.pixode.dynadoc.serialization

import java.time.Duration
import kotlinx.coroutines.time.delay
import org.pixode.dynadoc.core.UpdateConflictException

typealias RetryPolicy = suspend (throwable: Throwable, failureCount: Int) -> Boolean


suspend inline fun <T> EntityStore.transaction(
    noinline retryPolicy: RetryPolicy = NO_RETRY,
    execute: BatchBuilder.() -> T,
): T {
    var failureCount: Int = 0

    while (true) {
        val batchBuilder = BatchBuilder(this)
        val result = execute(batchBuilder)
        try {
            batchBuilder.submit()
            return result

        } catch (exception: Throwable) {
            failureCount++
            val retry: Boolean = retryPolicy(exception, failureCount)

            if (!retry) {
                throw exception
            }
        }
    }
}

val NO_RETRY: RetryPolicy = { _, _ -> false }

fun retry(maxRetries: Int, pause: Duration = Duration.ZERO): RetryPolicy =
    { throwable, failureCount ->
        if (throwable is UpdateConflictException && failureCount <= maxRetries) {
            delay(pause)
            true
        } else {
            false
        }
    }
