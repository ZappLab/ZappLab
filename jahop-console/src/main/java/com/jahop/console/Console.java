package com.jahop.console;

import com.jahop.api.Environment;
import com.jahop.api.Transport;
import org.jline.builtins.Options;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.impl.completer.EnumCompleter;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

public class Console {
    private static final String USAGE =
            "-t,--transport=TRANSPORT               Client transport - TCP, UDP (default=TCP)\n" +
                    "-e,--environment=ENV                   Client environment - LOCAL, DEV, UAT, PROD (default=LOCAL)\n" +
                    "-X,--debug                             Produce execution debug output\n" +
                    "-v,--version                           Display version information\n" +
                    "-h,--help                              Display help information\n";
    private static Options OPTIONS = Options.compile(USAGE);

    public static void main(String[] args) throws IOException {
        OPTIONS.parse(args);
        final Transport transport = Transport.valueOf(OPTIONS.get("transport"));
        final Environment env = Environment.valueOf(OPTIONS.get("environment"));

        final Context ctx = new Context(transport, env, OPTIONS.isSet("debug"));
        final Console console = new Console(ctx);
        console.start();
    }

    private final Pattern whitespace = Pattern.compile("\\w");
    private final AtomicBoolean started = new AtomicBoolean();
    private final Context ctx;

    private Terminal terminal;
    private LineReader lineReader;
    private Thread thread;

    public Console(Context ctx) {
        this.ctx = ctx;
    }

    void start() throws IOException {
        if (started.compareAndSet(false, true)) {
            terminal = TerminalBuilder.builder()
                    .system(true)
                    .streams(System.in, System.out)
                    .build();
            lineReader = LineReaderBuilder.builder()
                    .terminal(terminal)
                    .completer(new EnumCompleter(Command.class))
                    .build();
            thread = new Thread(this::run);
            thread.setName("console-thread");
            thread.start();
        }
    }

    void stop() throws IOException {
        if (started.compareAndSet(true, false)) {
            if (terminal != null) {
                terminal.flush();
                terminal.close();
            }
            if (thread != null) {
                try {
                    thread.join(1000);
                } catch (InterruptedException ignore) {
                }
            }
        }
    }

    void run() {
        final String prompt = String.format("%s %s> ", ctx.getTransport(), ctx.getEnvironment());
        while (started.get()) {
            final String line = lineReader.readLine(prompt);
            if (line != null) {
                final String[] args = line.split(" ");//whitespace.split(line);
                if (args.length > 0) {
                    final Command cmd = Command.parse(args[0]);
                    if (cmd != null) {
                        if (cmd == Command.exit) {
                            terminal.writer().println("Bye!");
                            started.set(false);
                        }
                        terminal.writer().println(cmd + " blah blah blah");
                    } else {
                        terminal.writer().println("Unknown command: " + line);
                    }
                }
            }
            terminal.flush();
        }
    }
}
