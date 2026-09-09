package org.limitless.phixeron.tools;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.SchemaFactory;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

/**
 * The deployment document {@code clusterctl load-topology} publishes (spec §6.4), read and validated. Its
 * three sections are the complete producer view of a deployment: the elected gateway pairs, the co-located
 * applications, and the protocol registry naming each shared {@code payloadId}.
 *
 * <p>Split out of {@link ClusterCtl} for the reason {@code Sequencer} and {@code TapPublisher} are: this
 * half touches no Aeron and no cluster, so it is unit-testable directly — which matters because every check
 * here is one an operator only ever meets as a refusal, and a check that silently stopped applying would
 * look exactly like a document that was fine.
 *
 * <p><b>An instance of this class is a valid document.</b> {@link #read} parses and validates in one step,
 * before {@link ClusterCtl} publishes a byte: a file that failed half-way through publishing would leave a
 * gateway list the log has already closed.
 */
public record TopologyDocument(List<TopologyRow> gateways, List<ApplicationRow> applications,
                               List<ProtocolRow> protocols) {
    /** One list row: an elected gateway pair's instance. */
    public record TopologyRow(String gatewayName, int gatewayId, int gatewaySourceId, int preferenceRank) { }

    /** One co-located application: the producer kind that is not elected (§5). */
    public record ApplicationRow(String applicationName, int sourceId) { }

    /** One protocol-registry row: what a shared payloadId is called in this deployment (§6.3). */
    public record ProtocolRow(int payloadId, int protocolVersion, String protocolName) { }

    private static final String TOPOLOGY_NS = "http://limitless.org/seqeron/topology/1";

    /** {@code clusterctl}'s own {@code sourceId} for the markers it submits; never a deployment's. */
    static final int RESERVED_SOURCE_ID = 2;

    /**
     * Reads {@code file}, validated against the packaged {@code topology.xsd} as it parses and then against
     * the two checks a row cannot make about itself.
     * @throws SAXParseException        the document does not satisfy the schema — carries line and column
     * @throws IllegalArgumentException the document is well-formed and schema-valid but not a deployment
     */
    public static TopologyDocument read(final File file)
        throws IOException, SAXException, ParserConfigurationException {
        final Element root = parser().parse(file).getDocumentElement();
        final List<TopologyRow> gateways = new ArrayList<>();
        for (final Element row : childElements(root, "gateways", "gateway")) {
            gateways.add(new TopologyRow(row.getAttribute("name"),
                                         Integer.parseInt(row.getAttribute("id")),
                                         Integer.parseInt(row.getAttribute("sourceId")),
                                         Integer.parseInt(row.getAttribute("rank"))));
        }
        final List<ApplicationRow> applications = new ArrayList<>();
        for (final Element row : childElements(root, "applications", "application")) {
            applications.add(new ApplicationRow(row.getAttribute("name"),
                                                Integer.parseInt(row.getAttribute("sourceId"))));
        }
        final List<ProtocolRow> protocols = new ArrayList<>();
        for (final Element row : childElements(root, "protocols", "protocol")) {
            protocols.add(new ProtocolRow(Integer.parseInt(row.getAttribute("payloadId")),
                                          Integer.parseInt(row.getAttribute("version")),
                                          row.getAttribute("name")));
        }
        validate(gateways, applications);
        return new TopologyDocument(gateways, applications, protocols);
    }

    /**
     * Builds a parser that validates against the packaged {@code topology.xsd} as it reads.
     *
     * <p>The schema is resolved from this jar and a {@code schemaLocation} the document names is
     * ignored: a file that may name its own schema may name a lax one, and C-1 would be advisory
     * (§6.4). Entity resolution is disabled outright — the threat is mild, since an operator who can
     * edit the file already has a shell on the node, but the JDK's defaults are unsafe.
     */
    private static DocumentBuilder parser() throws IOException, SAXException, ParserConfigurationException {
        final SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        schemaFactory.setProperty(XMLConstants.ACCESS_EXTERNAL_DTD, "");
        schemaFactory.setProperty(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
        final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        try (InputStream xsd = TopologyDocument.class.getResourceAsStream("/topology.xsd")) {
            if (null == xsd) {
                throw new IOException("topology.xsd missing from the clusterctl jar");
            }
            factory.setSchema(schemaFactory.newSchema(new StreamSource(xsd)));
        }
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        factory.setXIncludeAware(false);
        factory.setExpandEntityReferences(false);
        factory.setNamespaceAware(true);
        final DocumentBuilder builder = factory.newDocumentBuilder();
        builder.setErrorHandler(new ErrorHandler() {
            @Override
            public void warning(final SAXParseException ex) throws SAXException {
                throw ex;
            }

            @Override
            public void error(final SAXParseException ex) throws SAXException {
                throw ex;
            }

            @Override
            public void fatalError(final SAXParseException ex) throws SAXException {
                throw ex;
            }
        });
        return builder;
    }

    /** The {@code <row>} elements of {@code root}'s {@code <section>}, in document order; empty if absent. */
    private static List<Element> childElements(final Element root, final String section, final String row) {
        final List<Element> elements = new ArrayList<>();
        final NodeList sections = root.getElementsByTagNameNS(TOPOLOGY_NS, section);
        if (sections.getLength() > 0) {
            final NodeList rows = ((Element)sections.item(0)).getElementsByTagNameNS(TOPOLOGY_NS, row);
            for (int i = 0; i < rows.getLength(); i++) {
                final Node node = rows.item(i);
                if (Node.ELEMENT_NODE == node.getNodeType()) {
                    elements.add((Element)node);
                }
            }
        }
        return elements;
    }

    /**
     * The checks the XSD cannot make, each about a row's relation to the others: exactly one rank-0
     * per gateway {@code sourceId} — a missing one leaves a logical gateway with no primary, a second
     * an arbitrary one — a {@code sourceId} that is none of §5's reserved ids, and an application
     * {@code sourceId} no gateway row already claims. Everything else (field widths, name shape,
     * uniqueness, {@code payloadId >= 2}) is declarative in topology.xsd.
     *
     * <p>The last is not cosmetic: an application sharing a listed {@code sourceId} is refused frame by
     * frame at run time by <b>S-6</b> case 2 — it submits on a session no {@code GatewayStarted} bound —
     * so the deployment would come up and then silently drop that application's traffic.
     */
    private static void validate(final List<TopologyRow> rows, final List<ApplicationRow> applications) {
        for (final TopologyRow row : rows) {
            if (RESERVED_SOURCE_ID == row.gatewaySourceId()) {
                throw new IllegalArgumentException(row.gatewayName() + ": sourceId " + RESERVED_SOURCE_ID +
                                                   " is reserved for clusterctl's own markers");
            }
            int primaries = 0;
            for (final TopologyRow other : rows) {
                if (other.gatewaySourceId() == row.gatewaySourceId() && other.preferenceRank() == 0) {
                    primaries++;
                }
            }
            if (primaries != 1) {
                throw new IllegalArgumentException("sourceId " + row.gatewaySourceId() + " has " + primaries +
                                                   " rank-0 row(s), needs exactly 1");
            }
        }
        for (final ApplicationRow application : applications) {
            if (RESERVED_SOURCE_ID == application.sourceId()) {
                throw new IllegalArgumentException(application.applicationName() + ": sourceId " +
                                                   RESERVED_SOURCE_ID + " is reserved for clusterctl's own markers");
            }
            for (final TopologyRow row : rows) {
                if (row.gatewaySourceId() == application.sourceId()) {
                    throw new IllegalArgumentException(application.applicationName() + ": sourceId " +
                                                       application.sourceId() + " is gateway " +
                                                       row.gatewayName() + "'s");
                }
            }
        }
    }
}
