import com.aeroncookbook.cluster.rsm.gen.Snapshot;
import exchange.core2.core.ExchangeCore;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import lombok.extern.slf4j.Slf4j;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;

@Slf4j
public class ExchangeCoreClusteredService implements ClusteredService {

    private final ExchangeCore exchangeCore;
    private final ExchangeCoreDemuxer exchangeCoreDemuxer;

    public ExchangeCoreClusteredService() {
        this.exchangeCore = ExchangeCore.builder().build();
        this.exchangeCoreDemuxer = new ExchangeCoreDemuxer(exchangeCore);
    }

    @Override
    public void onStart(Cluster cluster, Image snapshotImage) {
        if (snapshotImage != null) {
            log.info("loading snapshot");
            snapshotImage.poll(exchangeCoreDemuxer, 1);
        }
    }

    @Override
    public void onSessionOpen(ClientSession session, long timestamp) {
        log.info("Cluster Client Session opened");
    }

    @Override
    public void onSessionClose(ClientSession session, long timestamp, CloseReason closeReason) {
        log.info("Cluster Client Session closed");
    }

    @Override
    public void onSessionMessage(ClientSession session, long timestamp, DirectBuffer buffer, int offset,
                                 int length, Header header) {
        log.info("Received message with buffer {}", buffer);
        exchangeCoreDemuxer.setSession(session);
        exchangeCoreDemuxer.onFragment(buffer, offset, length, header);
    }

    @Override
    public void onTimerEvent(long correlationId, long timestamp) {
        log.info("Cluster Node timer firing");
    }

    @Override
    public void onTakeSnapshot(ExclusivePublication snapshotPublication) {
        log.info("taking snapshot");
        final ExpandableDirectByteBuffer buffer = new ExpandableDirectByteBuffer(Snapshot.BUFFER_LENGTH);

        // get the current exchange state and set push into buffer here
        // exchangeCore.takeSnapshot(buffer)

        // offer the snapshot to the publication
        snapshotPublication.offer(buffer, 0, Snapshot.BUFFER_LENGTH);
    }

    @Override
    public void onRoleChange(Cluster.Role newRole) {
        log.info("Cluster Node is in role {}", newRole.name());
    }

    @Override
    public void onTerminate(Cluster cluster) {
        log.info("Cluster Node is terminating");
    }
}
