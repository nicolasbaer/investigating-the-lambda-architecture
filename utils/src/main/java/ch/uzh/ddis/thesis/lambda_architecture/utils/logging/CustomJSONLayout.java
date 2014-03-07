package ch.uzh.ddis.thesis.lambda_architecture.utils.logging;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.helpers.Charsets;
import org.apache.logging.log4j.core.helpers.Throwables;
import org.apache.logging.log4j.core.helpers.Transform;
import org.apache.logging.log4j.core.layout.JSONLayout;
import org.apache.logging.log4j.message.Message;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
@Plugin(name = "CustomJSONLayout", category = "Core", elementType = "layout", printObject = true)
public class CustomJSONLayout extends JSONLayout {
    private final static Logger logger = LogManager.getLogger();

    private static final int DEFAULT_SIZE = 256;

    // We yield to \r\n for the default.
    private static final String DEFAULT_EOL = "\r\n";
    private static final String COMPACT_EOL = "";
    private static final String DEFAULT_INDENT = "  ";
    private static final String COMPACT_INDENT = "";

    private static final String[] FORMATS = new String[] { "json" };

    private final boolean locationInfo;
    private final boolean properties;
    private final boolean complete;
    private final String eol;
    private final String indent1;
    private final String indent2;
    private final String indent3;
    private final String indent4;
    private volatile boolean firstLayoutDone;

    protected CustomJSONLayout(final boolean locationInfo, final boolean properties, final boolean complete, boolean compact,
                         final Charset charset) {
        super(locationInfo, properties, complete, compact, charset);

        this.locationInfo = locationInfo;
        this.properties = properties;
        this.complete = complete;
        this.eol = compact ? COMPACT_EOL : DEFAULT_EOL;
        this.indent1 = compact ? COMPACT_INDENT : DEFAULT_INDENT;
        this.indent2 = this.indent1 + this.indent1;
        this.indent3 = this.indent2 + this.indent1;
        this.indent4 = this.indent3 + this.indent1;
    }

    /**
     * Formats a {@link org.apache.logging.log4j.core.LogEvent} in conformance with the log4j.dtd.
     *
     * @param event
     *            The LogEvent.
     * @return The XML representation of the LogEvent.
     */
    @Override
    public String toSerializable(final LogEvent event) {
        final StringBuilder buf = new StringBuilder(DEFAULT_SIZE);
        // DC locking to avoid synchronizing the whole layout.
        boolean check = this.firstLayoutDone;
        if (!this.firstLayoutDone) {
            synchronized(this) {
                check = this.firstLayoutDone;
                if (!check) {
                    this.firstLayoutDone = true;
                } else {
                    buf.append(',');
                    buf.append(this.eol);
                }
            }
        } else {
            buf.append(',');
            buf.append(this.eol);
        }
        buf.append(this.indent1);
        buf.append('{');
        buf.append(this.eol);
        buf.append(this.indent2);
        buf.append("\"logger\":\"");
        String name = event.getLoggerName();
        if (name.isEmpty()) {
            name = "root";
        }
        buf.append(Transform.escapeJsonControlCharacters(name));
        buf.append("\",");
        buf.append(this.eol);
        buf.append(this.indent2);
        buf.append("\"timestamp\":\"");
        buf.append(event.getMillis());
        buf.append("\",");
        buf.append(this.eol);
        buf.append(this.indent2);
        buf.append("\"level\":\"");
        buf.append(Transform.escapeJsonControlCharacters(String.valueOf(event.getLevel())));
        buf.append("\",");
        buf.append(this.eol);
        buf.append(this.indent2);
        buf.append("\"thread\":\"");
        buf.append(Transform.escapeJsonControlCharacters(event.getThreadName()));
        buf.append("\",");
        buf.append(this.eol);

        final Message msg = event.getMessage();
        if (msg != null) {
            buf.append(this.indent2);
            buf.append("\"message\":");
            buf.append(msg.getFormattedMessage());
            //buf.append('');
        }

        if (event.getContextStack().getDepth() > 0) {
            buf.append(",");
            buf.append(this.eol);
            buf.append("\"ndc\":");
            buf.append(Transform.escapeJsonControlCharacters(event.getContextStack().toString()));
            buf.append("\"");
        }

        final Throwable throwable = event.getThrown();
        if (throwable != null) {
            buf.append(",");
            buf.append(this.eol);
            buf.append(this.indent2);
            buf.append("\"throwable\":\"");
            final List<String> list = Throwables.toStringList(throwable);
            for (final String str : list) {
                buf.append(Transform.escapeJsonControlCharacters(str));
                buf.append("\\\\n");
            }
            buf.append("\"");
        }

        if (this.locationInfo) {
            final StackTraceElement element = event.getSource();
            buf.append(",");
            buf.append(this.eol);
            buf.append(this.indent2);
            buf.append("\"LocationInfo\":{");
            buf.append(this.eol);
            buf.append(this.indent3);
            buf.append("\"class\":\"");
            buf.append(Transform.escapeJsonControlCharacters(element.getClassName()));
            buf.append("\",");
            buf.append(this.eol);
            buf.append(this.indent3);
            buf.append("\"method\":\"");
            buf.append(Transform.escapeJsonControlCharacters(element.getMethodName()));
            buf.append("\",");
            buf.append(this.eol);
            buf.append(this.indent3);
            buf.append("\"file\":\"");
            buf.append(Transform.escapeJsonControlCharacters(element.getFileName()));
            buf.append("\",");
            buf.append(this.eol);
            buf.append(this.indent3);
            buf.append("\"line\":\"");
            buf.append(element.getLineNumber());
            buf.append("\"");
            buf.append(this.eol);
            buf.append(this.indent2);
            buf.append("}");
        }

        if (this.properties && event.getContextMap().size() > 0) {
            buf.append(",");
            buf.append(this.eol);
            buf.append(this.indent2);
            buf.append("\"Properties\":[");
            buf.append(this.eol);
            final Set<Map.Entry<String, String>> entrySet = event.getContextMap().entrySet();
            int i = 1;
            for (final Map.Entry<String, String> entry : entrySet) {
                buf.append(this.indent3);
                buf.append('{');
                buf.append(this.eol);
                buf.append(this.indent4);
                buf.append("\"name\":\"");
                buf.append(Transform.escapeJsonControlCharacters(entry.getKey()));
                buf.append("\",");
                buf.append(this.eol);
                buf.append(this.indent4);
                buf.append("\"value\":\"");
                buf.append(Transform.escapeJsonControlCharacters(String.valueOf(entry.getValue())));
                buf.append("\"");
                buf.append(this.eol);
                buf.append(this.indent3);
                buf.append("}");
                if (i < entrySet.size()) {
                    buf.append(",");
                }
                buf.append(this.eol);
                i++;
            }
            buf.append(this.indent2);
            buf.append("]");
        }

        buf.append(this.eol);
        buf.append(this.indent1);
        buf.append("}");

        logger.info(buf.toString());
        return buf.toString();
    }

    @PluginFactory
    public static JSONLayout createLayout(
            @PluginAttribute("locationInfo") final String locationInfo,
            @PluginAttribute("properties") final String properties,
            @PluginAttribute("complete") final String completeStr,
            @PluginAttribute("compact") final String compactStr,
            @PluginAttribute("charset") final String charsetName) {
        final Charset charset = Charsets.getSupportedCharset(charsetName, Charsets.UTF_8);
        final boolean info = Boolean.parseBoolean(locationInfo);
        final boolean props = Boolean.parseBoolean(properties);
        final boolean complete = Boolean.parseBoolean(completeStr);
        final boolean compact = Boolean.parseBoolean(compactStr);
        return new CustomJSONLayout(info, props, complete, compact, charset);
    }

}
