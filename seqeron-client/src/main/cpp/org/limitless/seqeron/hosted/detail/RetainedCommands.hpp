#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>

namespace org::limitless::seqeron::hosted::detail {

/**
 * The Publish commands an Application stub has offered and whose fate the host has not yet reported. Each is kept, encoded, in one arena, in offer order, until `Resolved` passes its sequence number; one that a
 * `Dropped` named stays, flagged, to be resent under a new sequence number and epoch. A resent command is appended
 * after the others, so each entry also carries its rank, the order it was first offered in, which resends keep.
 *
 * Holds no Aeron object and allocates only in reset(). Single-threaded.
 */
class RetainedCommands
{
  public:
    /**
     * Whether a command stamped with an epoch before this one is still undecided. A resend waits for none, or a
     * command the host has yet to report would be resent after the ones that followed it.
     */
    [[nodiscard]] bool hasUndecidedBefore(const std::int64_t epoch) const noexcept
    {
        for (std::size_t i = 0; i < m_count; i++)
        {
            const Entry& entry = m_entries[(m_head + i) % m_entries.size()];
            if (entry.epoch >= epoch)
            {
                break;
            }
            if (entry.state == State::Live)
            {
                return true;
            }
        }
        return false;
    }

    /** The oldest flagged command's bytes, for the caller to patch and offer; empty when none is flagged. */
    struct Flagged
    {
        char* data = nullptr;
        std::size_t length = 0;
    };

    /** Forgets everything and sizes the arena and the table of entries. */
    void reset(const std::size_t arenaBytes, const std::size_t maxEntries)
    {
        m_arena.assign(arenaBytes, 0);
        m_entries.assign(maxEntries, Entry{});
        m_head = 0;
        m_count = 0;
        m_tail = 0;
        m_flagged = 0;
    }

    /** Whether retain() of a command this long will find room. */
    [[nodiscard]] bool canRetain(const std::size_t length) const noexcept
    {
        std::size_t offset = 0;
        return fits(length, offset);
    }

    /** Keeps a copy of the command just offered; canRetain(length) must have held before the offer. */
    void retain(const std::int64_t seq, const std::int64_t epoch, const char* message, const std::size_t length)
    {
        std::size_t offset = 0;
        fits(length, offset);
        std::memcpy(m_arena.data() + offset, message, length);
        append(seq, epoch, seq, offset, length);
    }

    /** `Dropped`: the retained commands firstSeq to lastSeq were not placed and are to be resent. */
    void dropped(const std::int64_t firstSeq, const std::int64_t lastSeq)
    {
        for (std::size_t i = 0; i < m_count; i++)
        {
            Entry& entry = at(i);
            if (entry.seq > lastSeq)
            {
                break;
            }
            if (entry.seq >= firstSeq && entry.state == State::Live)
            {
                entry.state = State::Flagged;
                m_flagged++;
            }
        }
    }

    /** `Resolved`: every command up to `through` is decided, and those not flagged were placed. */
    void resolved(const std::int64_t through)
    {
        for (std::size_t i = 0; i < m_count; i++)
        {
            Entry& entry = at(i);
            if (entry.seq > through)
            {
                break;
            }
            if (entry.state == State::Live)
            {
                entry.state = State::Free;
            }
        }
        reclaim();
    }

    [[nodiscard]] bool hasFlagged() const noexcept
    {
        return m_flagged != 0;
    }

    /**
     * Whether a new publish would overtake a retained one: a command waits to be resent, or one stamped with an
     * earlier epoch is undecided and may yet be dropped. Live entries' epochs do not fall, so the head is the oldest.
     */
    [[nodiscard]] bool blocksPublish(const std::int64_t epoch) const noexcept
    {
        if (m_count == 0)
        {
            return false;
        }
        return m_flagged != 0 || (m_entries[m_head].state == State::Live && m_entries[m_head].epoch < epoch);
    }

    /** The flagged command first offered earliest; its bytes stay valid until resent() or the next retain(). */
    [[nodiscard]] Flagged flagged()
    {
        const Entry* first = nullptr;
        for (std::size_t i = 0; i < m_count; i++)
        {
            const Entry& entry = at(i);
            if (entry.state == State::Flagged && (first == nullptr || entry.rank < first->rank))
            {
                first = &entry;
            }
        }
        return first == nullptr ? Flagged{} : Flagged{ m_arena.data() + first->offset, first->length };
    }

    /**
     * The command flagged() returned, patched and offered, is retained again under its new sequence number and epoch,
     * after every other, and the flagged one is released. canRetain(its length) must have held before the offer.
     */
    void resent(const std::int64_t seq, const std::int64_t epoch)
    {
        Entry* first = nullptr;
        for (std::size_t i = 0; i < m_count; i++)
        {
            Entry& entry = at(i);
            if (entry.state == State::Flagged && (first == nullptr || entry.rank < first->rank))
            {
                first = &entry;
            }
        }
        if (first == nullptr)
        {
            return;
        }
        const std::size_t length = first->length;
        const std::int64_t rank = first->rank;
        std::size_t offset = 0;
        fits(length, offset);
        std::memcpy(m_arena.data() + offset, m_arena.data() + first->offset, length);
        first->state = State::Free;
        m_flagged--;
        append(seq, epoch, rank, offset, length);
        reclaim();
    }

    [[nodiscard]] std::size_t count() const noexcept
    {
        return m_count;
    }

    /** Bytes the retained commands occupy, with any gap a wrap left. */
    [[nodiscard]] std::size_t bytesInUse() const noexcept
    {
        if (m_count == 0)
        {
            return 0;
        }
        const std::size_t head = m_entries[m_head].offset;
        return m_tail > head ? m_tail - head : m_arena.size() - head + m_tail;
    }

  private:
    enum class State : std::uint8_t
    {
        Live,
        Flagged,
        Free,
    };

    struct Entry
    {
        std::int64_t seq = 0;
        std::int64_t epoch = 0;
        std::int64_t rank = 0;
        std::size_t offset = 0;
        std::size_t length = 0;
        State state = State::Free;
    };

    Entry& at(const std::size_t i)
    {
        return m_entries[(m_head + i) % m_entries.size()];
    }

    // The offset a command of this length would take, if there is room: after the newest, or at the start when the
    // end is too short (the gap left is reclaimed with the head).
    bool fits(const std::size_t length, std::size_t& offset) const noexcept
    {
        if (m_count == m_entries.size() || length == 0 || length > m_arena.size())
        {
            return false;
        }
        if (m_count == 0)
        {
            offset = 0;
            return true;
        }
        const std::size_t head = m_entries[m_head].offset;
        if (m_tail > head)
        {
            if (length <= m_arena.size() - m_tail)
            {
                offset = m_tail;
                return true;
            }
            offset = 0;
            return length <= head;
        }
        offset = m_tail;
        return length <= head - m_tail;
    }

    void append(const std::int64_t seq, const std::int64_t epoch, const std::int64_t rank, const std::size_t offset,
                const std::size_t length)
    {
        m_entries[(m_head + m_count) % m_entries.size()] = Entry{ seq, epoch, rank, offset, length, State::Live };
        m_count++;
        m_tail = offset + length;
    }

    void reclaim()
    {
        while (m_count > 0 && m_entries[m_head].state == State::Free)
        {
            m_head = (m_head + 1) % m_entries.size();
            m_count--;
        }
        if (m_count == 0)
        {
            m_head = 0;
            m_tail = 0;
        }
    }

    std::vector<char> m_arena;
    std::vector<Entry> m_entries;
    std::size_t m_head = 0;
    std::size_t m_count = 0;
    std::size_t m_tail = 0;
    std::size_t m_flagged = 0;
};

} // namespace org::limitless::seqeron::hosted::detail
