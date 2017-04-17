package com.jahop.console;

import com.google.common.base.Splitter;
import com.jahop.api.Client;
import com.jahop.api.ClientFactory;
import com.jahop.api.Environment;
import com.jahop.api.Transport;
import com.jahop.common.msg.proto.Messages;
import org.jline.builtins.Options;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.impl.completer.EnumCompleter;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

import java.io.IOException;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public final class Console {
    private static final String VERSION = "JaHOP Console v0.1";
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

    private final Messages.Update.Builder updateBuilder = Messages.Update.newBuilder();
    private final Messages.EntrySet.Builder entrySetBuilder = Messages.EntrySet.newBuilder();
    private final Messages.Entry.Builder entryBuilder = Messages.Entry.newBuilder();

    private final Splitter splitter = Splitter.on(' ').omitEmptyStrings().trimResults();
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean connected = new AtomicBoolean();
    private final EnumMap<Command, Consumer<Iterator<String>>> handlers = new EnumMap<>(Command.class);
    private final Context ctx;

    private Terminal terminal;
    private LineReader lineReader;
    private Thread thread;
    private Client client;

    public Console(Context ctx) {
        this.ctx = ctx;
        registerHandlers();
    }


    private void registerHandlers() {
        handlers.put(Command.connect, this::connect);
        handlers.put(Command.disconnect, this::disconnect);
        handlers.put(Command.subscribe, this::subscribe);
        handlers.put(Command.unsubscribe, this::unsubscribe);
        handlers.put(Command.select, this::select);
        handlers.put(Command.update, this::update);
        handlers.put(Command.delete, this::delete);
        handlers.put(Command.help, this::help);
        handlers.put(Command.version, this::version);
        handlers.put(Command.exit, this::exit);
    }

    private void connect(final Iterator<String> args) {
        if (connected.compareAndSet(false, true)) {
            final ClientFactory factory = ClientFactory.newInstance(ctx.getTransport(), ctx.getEnvironment());
            client = factory.create(1000);
            client.start();
            printf("Connected to %s %s.\n",
                    ctx.getTransport().toString(), ctx.getEnvironment().toString());
        } else {
            printf("Connected to %s %s. Please disconnect first.\n",
                    ctx.getTransport().toString(), ctx.getEnvironment().toString());
        }
    }

    private void disconnect(final Iterator<String> args) {
        if (connected.compareAndSet(true, false)) {
            client.stop();
            printf("Disconnected from %s %s\n",
                    ctx.getTransport().toString(), ctx.getEnvironment().toString());
        } else {
            println("Not connected");
        }
    }

    private void subscribe(final Iterator<String> args) {
        println("Not implemented");
    }

    private void unsubscribe(final Iterator<String> args) {
        println("Not implemented");
    }

    private void select(final Iterator<String> args) {
        if (args.hasNext()) {
            updateBuilder.clear().setAuthor(VERSION).setComment("");
            args.forEachRemaining(path -> {
                entrySetBuilder.clear().setPath(path);
                updateBuilder.addEntrySet(entrySetBuilder.build());
            });
            final Messages.Update update = updateBuilder.build();
            client.send(update);
        } else {
            println(Command.select.getDescription());
        }
    }

    private void update(final Iterator<String> args) {
        Messages.Update update = null;
        if (args.hasNext()) {
            final String path = args.next();
            if (args.hasNext()) {
                final String key = args.next();
                if (args.hasNext()) {
                    final String value = args.next();
                    entryBuilder.clear().setKey(key).setValue(value).setAction(Messages.Entry.Action.UPDATE);
                    entrySetBuilder.clear().setPath(path);
                    entrySetBuilder.addEntry(entryBuilder.build());
                    updateBuilder.clear().setAuthor(VERSION).setComment("");
                    updateBuilder.addEntrySet(entrySetBuilder.build());
                    update = updateBuilder.build();
                }
            }
        }
        if (update != null) {
            client.send(update);
        } else {
            println(Command.update.getDescription());
        }
    }

    private void delete(final Iterator<String> args) {
        Messages.Update update = null;
        if (args.hasNext()) {
            final String path = args.next();
            if (args.hasNext()) {
                final String key = args.next();
                entryBuilder.clear().setKey(key).setAction(Messages.Entry.Action.DELETE);
                entrySetBuilder.clear().setPath(path);
                entrySetBuilder.addEntry(entryBuilder.build());
                updateBuilder.clear().setAuthor(VERSION).setComment("");
                updateBuilder.addEntrySet(entrySetBuilder.build());
                update = updateBuilder.build();
            }
        }
        if (update != null) {
            client.send(update);
        } else {
            println(Command.delete.getDescription());
        }
    }

    private void help(final Iterator<String> args) {
        Command cmd = null;
        if (args.hasNext()) {
            cmd = Command.parse(args.next());
        }
        if (cmd != null) {
            println(cmd.getDescription());
        } else {
            for (Command command : Command.values()) {
                println(command.getDescription());
            }
        }
    }

    private void version(final Iterator<String> args) {
        println(VERSION);
    }

    private void exit(final Iterator<String> args) {
        println("Bye!");
        started.set(false);
    }

    synchronized void start() throws IOException {
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
            connect(null);
        }
    }

    synchronized void stop() throws IOException {
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
            disconnect(null);
        }
    }

    void run() {
        final String prompt = String.format("%s %s> ", ctx.getTransport(), ctx.getEnvironment());
        while (started.get()) {
            final String line = lineReader.readLine(prompt);
            if (line != null) {
                final Iterator<String> args = splitter.split(line).iterator();
                if (args.hasNext()) {
                    final Command cmd = Command.parse(args.next());
                    if (cmd != null) {
                        handlers.get(cmd).accept(args);
                    } else {
                        println("Unknown command: " + line);
                    }
                }
            }
            terminal.flush();
        }
    }

    private void println(String text) {
        terminal.writer().println(text);
    }

    private void printf(String format, Object... args) {
        terminal.writer().printf(format, args);
    }
}
