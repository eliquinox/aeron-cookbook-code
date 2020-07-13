import exchange.core2.core.ExchangeCore;
import exchange.core2.core.common.cmd.OrderCommandType;
import io.aeron.cluster.service.ClientSession;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import lombok.extern.slf4j.Slf4j;
import org.agrona.DirectBuffer;

@Slf4j
public class ExchangeCoreDemuxer implements FragmentHandler {

    private final ExchangeCore exchangeCore;
    private ClientSession session;

    public ExchangeCoreDemuxer(ExchangeCore exchangeCore) {
        this.exchangeCore = exchangeCore;
    }

    @Override
    public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
        byte eiderId = buffer.getByte(offset);
        OrderCommandType cmdType = OrderCommandType.fromCode(eiderId);
        log.info("Received {} command", cmdType);
    }

    public void setSession(ClientSession session)
    {
        this.session = session;
    }
}
