#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <chrono>

namespace {

void check(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

std::string run_and_interrupt(const char* executable, int& status,
                              const char* holdback_ms = nullptr) {
    int pipe_fds[2];
    check(::pipe(pipe_fds) == 0, "output pipe creation failed");
    const auto child = ::fork();
    check(child >= 0, "fork failed");
    if (child == 0) {
        ::close(pipe_fds[0]);
        ::dup2(pipe_fds[1], STDOUT_FILENO);
        ::dup2(pipe_fds[1], STDERR_FILENO);
        ::close(pipe_fds[1]);
        if (holdback_ms) {
            ::execl(executable, executable, "--holdback-ms", holdback_ms,
                    static_cast<char*>(nullptr));
        } else {
            ::execl(executable, executable, static_cast<char*>(nullptr));
        }
        std::_Exit(127);
    }
    ::close(pipe_fds[1]);
    std::this_thread::sleep_for(std::chrono::milliseconds(2200));
    ::kill(child, SIGINT);

    bool exited = false;
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::seconds(8);
    while (std::chrono::steady_clock::now() < deadline) {
        const auto result = ::waitpid(child, &status, WNOHANG);
        if (result == child) {
            exited = true;
            break;
        }
        check(result >= 0, "waitpid failed");
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    if (!exited) {
        ::kill(child, SIGKILL);
        ::waitpid(child, &status, 0);
    }

    std::string output;
    char bytes[4096];
    while (const auto count = ::read(pipe_fds[0], bytes, sizeof(bytes))) {
        if (count < 0) break;
        output.append(bytes, static_cast<std::size_t>(count));
    }
    ::close(pipe_fds[0]);
    check(exited, "SIGINT shutdown timed out");
    return output;
}

}  // namespace

int main(int argc, char** argv) {
    try {
        check(argc == 2, "expected executable path");
        int status = 0;
        const auto output = run_and_interrupt(argv[1], status);
        check(WIFEXITED(status) && WEXITSTATUS(status) == 0,
              "application did not exit cleanly after SIGINT");
        check(output.find("holdback=10ms") != std::string::npos &&
                  output.find("interval | emitted A=") != std::string::npos &&
                  output.find("         | events B[") != std::string::npos &&
                  output.find("events A[") == std::string::npos &&
                  output.find("cumulative:") == std::string::npos &&
                  output.find("final (valid received = emitted + dropped):") !=
                      std::string::npos &&
                  output.find("accounting=ok") != std::string::npos,
              "missing compact interval or final report");
        const auto total = output.rfind("  total received=");
        check(total != std::string::npos, "missing final totals");
        unsigned long long received = 0, emitted = 0, dropped = 0, rejected = 0;
        check(std::sscanf(output.c_str() + total,
                          "  total received=%llu emitted=%llu dropped=%llu rejected=%llu",
                          &received, &emitted, &dropped, &rejected) == 4,
              "cannot parse final totals");
        check(received > 0 && emitted > 0 && rejected > 0 &&
                  received == emitted + dropped,
              "final accounting does not balance");
        const auto interval = output.find("interval | emitted A=");
        std::cout << output.substr(interval, output.find('\n', interval) - interval)
                  << '\n';
        std::cout << "SIGINT run: received=" << received
                  << " emitted=" << emitted << " dropped=" << dropped
                  << " rejected=" << rejected << " accounting=ok\n";
        int override_status = 0;
        const auto override_output = run_and_interrupt(argv[1], override_status,
                                                       "15");
        check(WIFEXITED(override_status) &&
                  WEXITSTATUS(override_status) == 0 &&
                  override_output.find("holdback=15ms") != std::string::npos &&
                  override_output.find("accounting=ok") != std::string::npos,
              "configurable holdback SIGINT run failed");
        std::cout << "15 ms override: accounting=ok\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "App test failed: " << error.what() << '\n';
        return 1;
    }
}
