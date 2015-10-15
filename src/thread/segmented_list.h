/* <thread/segmented_list.h>

   ----------------------------------------------------------------------------
   Copyright 2015 Dave Peterson <dave@dspeterson.com>

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
   ----------------------------------------------------------------------------

   Segmented list class.  This is part of the implementation of
   TManagedThreadPoolBase.
 */

#include <cassert>
#include <cstddef>
#include <list>
#include <utility>

#include <base/no_copy_semantics.h>

namespace Thread {

  /* Segmented list class.  TManagedThreadPoolBase uses this to manage its list
     of idle threads, which are divided into segments representing time
     intervals.  Initially there is only a single segment, and the segment
     count S grows until a maximum of Smax segments is reached.  The segments
     are ordered, and each segment contains an ordered list of idle threads.
     When a thread becomes idle, it is added to the front of the first segment.
     To allocate a thread for performing work, the first thread from the first
     nonempty segment is removed.  A manager thread wakes up every T units of
     time and prunes the idle list as follows:

         if (S < Smax) {
           Add a new empty segment to the front of the list, effectively
           shifting all other segments back one position.
         } else {
           Prune 0 or more threads from the last segment, depending on
           configuration settings.

           if (S > 1) {
             Move any remaining threads from the last segment to the end of the
             next to last segment.  Then rotate the (now empty) last segment to
             the front, effectively shifting all other segments back one
             position.
           }
         }

      Thus, threads become elgible for pruning once they have been idle for a
      certain length of time.  Values Smax and T determine how long a thread
      must remain idle to become elgible for pruning, and whether the pruning
      is done in a coarse or fine-grained manner.  Operations are implemented
      to be O(1) whenever possible. */
  template <typename T>
  class TSegmentedList final {
    NO_COPY_SEMANTICS(TSegmentedList);

    public:
    TSegmentedList()
        : AllSegments(1),
          NumSegments(1),
          TotalItems(0),
          FirstNonempty(nullptr),
          LastNonempty(nullptr) {
    }

    /* Return true if the list contains 0 items, or false otherwise.  This is
       independent of the number of segments.  For instance, true is returned
       even if there are many segments, as long as all segments are empty. */
    bool Empty() const noexcept {
      assert(this);
      assert(!AllSegments.empty());
      assert(NumSegments > 0);
      assert((FirstNonempty == nullptr) == (LastNonempty == nullptr));
      assert((FirstNonempty == nullptr) == (TotalItems == 0));
      return (TotalItems == 0);
    }

    /* Return the total number of items in all segments (not the number of
       segments). */
    size_t Size() const noexcept {
      assert(this);
      assert(!AllSegments.empty());
      assert(NumSegments > 0);
      assert((FirstNonempty == nullptr) == (LastNonempty == nullptr));
      assert((FirstNonempty == nullptr) == (TotalItems == 0));
      return TotalItems;
    }

    /* Return the number of segments.  Note that some or all segments may be
       empty.  The list always contains at least one segment. */
    size_t SegmentCount() const noexcept {
      assert(this);
      assert(!AllSegments.empty());
      assert(NumSegments > 0);
      assert((FirstNonempty == nullptr) == (LastNonempty == nullptr));
      assert((FirstNonempty == nullptr) == (TotalItems == 0));
      return NumSegments;
    }

    /* Copy 'item' to front of first segment. */
    void AddNew(const T &item) {
      assert(this);
      assert(!AllSegments.empty());
      assert(NumSegments > 0);
      assert((FirstNonempty == nullptr) == (LastNonempty == nullptr));
      assert((FirstNonempty == nullptr) == (TotalItems == 0));
      TSegment &first_seg = AllSegments.front();
      first_seg.AddToFront(item);
      ++TotalItems;

      if (first_seg.Size() == 1) {
        AddFirstSegToNonemptySegList();
      }
    }

    /* Move 'item' to front of first segment. */
    void AddNew(T &&item) {
      assert(this);
      assert(!AllSegments.empty());
      assert(NumSegments > 0);
      assert((FirstNonempty == nullptr) == (LastNonempty == nullptr));
      assert((FirstNonempty == nullptr) == (TotalItems == 0));
      TSegment &first_seg = AllSegments.front();
      first_seg.AddToFront(std::move(item));
      ++TotalItems;

      if (first_seg.Size() == 1) {
        AddFirstSegToNonemptySegList();
      }
    }

    /* Move contents of 'item_list' to front of first segment. */
    void AddNew(std::list<T> &item_list) noexcept {
      assert(this);
      assert(!AllSegments.empty());
      assert(NumSegments > 0);
      assert((FirstNonempty == nullptr) == (LastNonempty == nullptr));
      assert((FirstNonempty == nullptr) == (TotalItems == 0));
      size_t num_to_add = item_list.size();
      TSegment &first_seg = AllSegments.front();
      first_seg.AddToFront(item_list);
      TotalItems += num_to_add;

      if (num_to_add && (first_seg.Size() == num_to_add)) {
        AddFirstSegToNonemptySegList();
      }
    }

    /* Remove and return first item from first nonempty segment.  If list is
       initially empty, returned list will be empty.  Otherwise returned list
       will contain exactly one item. */
    std::list<T> RemoveOneNewest() noexcept {
      assert(this);
      assert(!AllSegments.empty());
      assert(NumSegments > 0);
      assert((FirstNonempty == nullptr) == (LastNonempty == nullptr));
      assert((FirstNonempty == nullptr) == (TotalItems == 0));
      std::list<T> result;

      if (TotalItems) {
        TSegment &seg = *FirstNonempty;
        assert(!seg.Empty());
        seg.TakeFront(result);
        assert(result.size() == 1);
        --TotalItems;

        if (seg.Empty()) {
          RemoveFrontItemFromNonemptySegList();
        }
      }

      return std::move(result);
    }

    /* Remove and return all items from last segment.  Returned list will be
       empty if last segment was empty. */
    std::list<T> EmptyOldest() noexcept {
      assert(this);
      assert(!AllSegments.empty());
      assert(NumSegments > 0);
      assert((FirstNonempty == nullptr) == (LastNonempty == nullptr));
      assert((FirstNonempty == nullptr) == (TotalItems == 0));
      TSegment &oldest = AllSegments.back();
      assert(oldest.Size() <= TotalItems);
      TotalItems -= oldest.Size();

      if (!oldest.Empty()) {
        RemoveLastSegFromNonemptySegList();
      }

      return oldest.TakeAll();
    }

    /* Remove and return up to 'max_count' items from the tail end of the last
       segment.  Returned list will have fewer than 'max_count' items (possibly
       0) if oldest segment has fewer items than requested. */
    std::list<T> RemoveOldest(size_t max_count) noexcept {
      assert(this);
      assert(!AllSegments.empty());
      assert(NumSegments > 0);
      assert((FirstNonempty == nullptr) == (LastNonempty == nullptr));
      assert((FirstNonempty == nullptr) == (TotalItems == 0));
      TSegment &oldest = AllSegments.back();
      size_t initial_size = oldest.Size();
      std::list<T> result = oldest.TakeUpToLastN(max_count);
      size_t final_size = oldest.Size();
      assert(final_size <= initial_size);
      TotalItems -= initial_size - final_size;

      if (initial_size && (final_size == 0)) {
        RemoveLastSegFromNonemptySegList();
      }

      return std::move(result);
    }

    /* Remove and return all items from all segments, but don't change the
       number of segments. */
    std::list<T> EmptyAll() noexcept {
      assert(this);
      assert(!AllSegments.empty());
      assert(NumSegments > 0);
      assert((FirstNonempty == nullptr) == (LastNonempty == nullptr));
      assert((FirstNonempty == nullptr) == (TotalItems == 0));
      std::list<T> result;
      TSegment *next = nullptr;

      for (TSegment *segp = FirstNonempty; segp; segp = next) {
        assert(!segp->Empty());
        next = segp->NextNonempty;
        segp->PrevNonempty = nullptr;
        segp->NextNonempty = nullptr;
        segp->TakeAll(result, result.end());
      }

      TotalItems = 0;
      FirstNonempty = nullptr;
      LastNonempty = nullptr;
      return std::move(result);
    }

    /* Remove and return all items from all segments, and reset the segment
       count to 1. */
    std::list<T> EmptyAllAndResetSegments() noexcept {
      assert(this);
      assert(!AllSegments.empty());
      assert(NumSegments > 0);
      assert((FirstNonempty == nullptr) == (LastNonempty == nullptr));
      assert((FirstNonempty == nullptr) == (TotalItems == 0));
      std::list<T> result = EmptyAll();
      AllSegments.resize(1);
      NumSegments = 1;
      TSegment &seg = AllSegments.front();
      seg.PrevNonempty = nullptr;
      seg.NextNonempty = nullptr;
      FirstNonempty = nullptr;
      LastNonempty = nullptr;
      return std::move(result);
    }

    /* Add a new empty segment, which becomes the first segment. */
    void AddNewSegment() {
      assert(this);
      assert(!AllSegments.empty());
      assert(NumSegments > 0);
      assert((FirstNonempty == nullptr) == (LastNonempty == nullptr));
      assert((FirstNonempty == nullptr) == (TotalItems == 0));
      AllSegments.emplace_front();
      ++NumSegments;
    }

    /* If there is only one segment, do nothing.  Otherwise, move all items
       from the last segment to the end of the next to last segment.  Then move
       the (now empty) last segment to the front. */
    void RecycleOldestSegment() noexcept {
      assert(this);
      assert(!AllSegments.empty());
      assert(NumSegments > 0);
      assert((FirstNonempty == nullptr) == (LastNonempty == nullptr));
      assert((FirstNonempty == nullptr) == (TotalItems == 0));

      if (&AllSegments.back() == &AllSegments.front()) {
        return;  // only one segment: nothing to do
      }

      auto seg_iter = AllSegments.end();
      --seg_iter;
      auto last_pos = seg_iter;
      bool prev_became_nonempty = false;

      {
        TSegment &last = *seg_iter;
        assert(seg_iter != AllSegments.begin());
        --seg_iter;
        bool last_was_nonempty = !last.Empty();
        prev_became_nonempty = seg_iter->Empty() && last_was_nonempty;
        last.AppendAllTo(*seg_iter);

        if (last_was_nonempty) {
          /* Before we rotate last segment to front, remove it from list of
             nonempty segments. */
          RemoveLastSegFromNonemptySegList();
        }
      }

      AllSegments.splice(AllSegments.begin(), AllSegments, last_pos);

      if (prev_became_nonempty) {
        /* The new last segment just became nonempty, so add it to list of
           nonempty segments. */
        AddLastSegToNonemptySegList();
      }
    }

    /* Reorganize list so all items are in a single segment, maintaining their
       order.  On return, the segment count will be 1. */
    void ResetSegments() noexcept {
      assert(this);
      assert(!AllSegments.empty());
      assert(NumSegments > 0);
      assert((FirstNonempty == nullptr) == (LastNonempty == nullptr));
      assert((FirstNonempty == nullptr) == (TotalItems == 0));
      TSegment &first = AllSegments.front();
      TSegment *segp = FirstNonempty;

      if (segp == &first) {
        assert(!segp->Empty());
        segp = segp->NextNonempty;
      }

      while (segp) {
        assert(!segp->Empty());
        TSegment *curr = segp;
        segp = segp->NextNonempty;
        curr->AppendAllTo(first);
      }

      assert(first.Size() == TotalItems);
      AllSegments.resize(1);
      NumSegments = 1;
      first.PrevNonempty = nullptr;
      first.NextNonempty = nullptr;

      if (first.Empty()) {
        FirstNonempty = nullptr;
        LastNonempty = nullptr;
      } else {
        FirstNonempty = &first;
        LastNonempty = &first;
      }
    }

    /* for testing */
    std::list<std::list<T>> CopyOutSegments() const {
      assert(this);
      assert(!AllSegments.empty());
      assert(AllSegments.size() == NumSegments);
      assert((FirstNonempty == nullptr) == (LastNonempty == nullptr));
      assert((FirstNonempty == nullptr) == (TotalItems == 0));
      std::list<std::list<T>> result;
      size_t item_count = 0;

      for (const TSegment &seg : AllSegments) {
        size_t seg_size = seg.Size();
        item_count += seg_size;
        result.push_back(seg.CopyOutContents());
        assert(result.back().size() == seg_size);
      }

      assert(item_count == TotalItems);
      return std::move(result);
    }

    /* for testing */
    bool SanityCheck() const {
      assert(this);

      if (NumSegments != AllSegments.size()) {
        return false;
      }

      size_t item_count = 0;

      for (const TSegment &seg : AllSegments) {
        if (!seg.SanityCheck()) {
          return false;
        }

        item_count += seg.Size();
      }

      if (item_count != TotalItems) {
        return false;
      }

      TSegment *p = nullptr;
      auto iter = AllSegments.begin();

      for (TSegment *n = FirstNonempty; n; p = n, n = n->NextNonempty) {
        if (n->PrevNonempty != p) {
          return false;
        }

        for (; (iter != AllSegments.end()) && (&*iter != n); ++iter) {
          if (!iter->Empty()) {
            return false;
          }
        }

        if (iter == AllSegments.end()) {
          return false;
        }

        if (iter->Empty()) {
          return false;
        }

        ++iter;
      }

      if (LastNonempty != p) {
        return false;
      }

      for (; iter != AllSegments.end(); ++iter) {
        if (!iter->Empty()) {
          return false;
        }
      }

      return true;
    }

    private:
    /* a single segment */
    class TSegment final {
      NO_COPY_SEMANTICS(TSegment);

      public:
      TSegment() noexcept
          : PrevNonempty(nullptr),
            NextNonempty(nullptr),
            NumItems(0) {
      }

      /* Return true if segment is empty or false otherwise. */
      bool Empty() const noexcept {
        assert(this);
        assert(Items.empty() == (NumItems == 0));
        return (NumItems == 0);
      }

      /* Return number of items in segment. */
      size_t Size() const noexcept {
        assert(this);
        assert(Items.empty() == (NumItems == 0));
        return NumItems;
      }

      /* Copy 'item' to front of segment. */
      void AddToFront(const T &item) {
        assert(this);
        assert(Items.empty() == (NumItems == 0));
        Items.push_front(item);
        ++NumItems;
      }

      /* Move 'item' to front of segment. */
      void AddToFront(T &&item) {
        assert(this);
        assert(Items.empty() == (NumItems == 0));
        Items.push_front(std::move(item));
        ++NumItems;
      }

      /* Move 'item_list' to front of segment. */
      void AddToFront(std::list<T> &item_list) noexcept {
        assert(this);
        assert(Items.empty() == (NumItems == 0));
        size_t n = item_list.size();
        Items.splice(Items.begin(), item_list);
        NumItems += n;
      }

      /* Remove front item from segment and append it to 'dst'.  Segment must
         be initially nonempty. */
      void TakeFront(std::list<T> &dst) noexcept {
        assert(this);
        assert(NumItems);
        assert(!Items.empty());
        dst.splice(dst.end(), Items, Items.begin());
        --NumItems;
      }

      /* Remove all items from segment and insert them into 'dst' at position
         'pos'. */
      void TakeAll(std::list<T> &dst,
          typename std::list<T>::iterator pos) noexcept {
        assert(this);
        assert(Items.empty() == (NumItems == 0));
        dst.splice(pos, Items);
        NumItems = 0;
      }

      /* Remove and return all items from segment. */
      std::list<T> TakeAll() noexcept {
        assert(this);
        assert(Items.empty() == (NumItems == 0));
        NumItems = 0;
        return std::move(Items);
      }

      /* Remove and return last 'n' items from segment, or all items if segment
         initially has fewer than 'n' items. */
      std::list<T> TakeUpToLastN(size_t n) noexcept {
        assert(this);
        assert(Items.empty() == (NumItems == 0));

        if (n >= NumItems) {
          return TakeAll();
        }

        auto start_pos = Items.end();

        for (size_t i = 0; i < n; ++i) {
          assert(start_pos != Items.begin());
          --start_pos;
        }

        std::list<T> result;
        result.splice(result.end(), Items, start_pos, Items.end());
        NumItems -= n;
        return std::move(result);
      }

      /* Remove all items and append them to 'dst'. */
      void AppendAllTo(TSegment &dst) noexcept {
        assert(this);
        assert(Items.empty() == (NumItems == 0));
        dst.Items.splice(dst.Items.end(), Items);
        dst.NumItems += NumItems;
        NumItems = 0;
      }

      /* for testing */
      std::list<T> CopyOutContents() const {
        assert(this);
        assert(Items.size() == NumItems);
        return Items;
      }

      /* for testing */
      bool SanityCheck() const {
        assert(this);
        return (Items.size() == NumItems);
      }

      /* When the segment is nonempty, these are used to link it into a list of
         all nonempty segments.  They are both null when the segment is empty.
       */
      TSegment *PrevNonempty;
      TSegment *NextNonempty;

      private:
      std::list<T> Items;

      /* Maintain separate item count, since Items.size() is not O(1). */
      size_t NumItems;
    };  // TSegment

    /* The first segment just became nonempty.  Add it to the front of the
       nonempty segment list. */
    void AddFirstSegToNonemptySegList() noexcept {
      assert(this);
      TSegment &first_seg = AllSegments.front();
      assert(FirstNonempty != &first_seg);
      assert(LastNonempty != &first_seg);
      assert(first_seg.PrevNonempty == nullptr);
      assert(first_seg.NextNonempty == nullptr);
      assert((FirstNonempty == nullptr) == (LastNonempty == nullptr));
      first_seg.NextNonempty = FirstNonempty;

      if (FirstNonempty) {
        FirstNonempty->PrevNonempty = &first_seg;
      } else {
        LastNonempty = &first_seg;
      }

      FirstNonempty = &first_seg;
      assert(FirstNonempty);
      assert(LastNonempty);
      assert(FirstNonempty->PrevNonempty == nullptr);
      assert(LastNonempty->NextNonempty == nullptr);
      assert((NumSegments > 1) || (FirstNonempty == LastNonempty));
    }

    /* Remove front item from nonempty segment list, since segment just became
       empty. */
    void RemoveFrontItemFromNonemptySegList() noexcept {
      assert(this);
      assert(NumSegments > 0);
      assert(FirstNonempty);
      assert(LastNonempty);
      assert(FirstNonempty->PrevNonempty == nullptr);
      assert(LastNonempty->NextNonempty == nullptr);
      assert((NumSegments > 1) || (FirstNonempty == LastNonempty));
      TSegment *next = FirstNonempty->NextNonempty;
      FirstNonempty->NextNonempty = nullptr;
      FirstNonempty = next;

      if (next) {
        next->PrevNonempty = nullptr;
      } else {
        LastNonempty = nullptr;
      }

      assert((FirstNonempty == nullptr) == (LastNonempty == nullptr));
    }

    /* The last segment just became empty.  Remove it from the back of the
       nonempty segment list. */
    void RemoveLastSegFromNonemptySegList() noexcept {
      assert(this);
      assert(NumSegments > 0);
      assert(FirstNonempty);
      assert(LastNonempty);
      assert(FirstNonempty->PrevNonempty == nullptr);
      assert(LastNonempty->NextNonempty == nullptr);
      assert((NumSegments > 1) || (FirstNonempty == LastNonempty));
      assert(LastNonempty == &AllSegments.back());
      TSegment *prev = LastNonempty->PrevNonempty;
      LastNonempty->PrevNonempty = nullptr;
      LastNonempty = prev;

      if (prev) {
        prev->NextNonempty = nullptr;
      } else {
        FirstNonempty = nullptr;
      }

      assert((FirstNonempty == nullptr) == (LastNonempty == nullptr));
    }

    /* The last segment just became nonempty.  Add it to the back of the
       nonempty segment list. */
    void AddLastSegToNonemptySegList() noexcept {
      assert(this);
      TSegment &last_seg = AllSegments.back();
      assert(FirstNonempty != &last_seg);
      assert(LastNonempty != &last_seg);
      assert(last_seg.PrevNonempty == nullptr);
      assert(last_seg.NextNonempty == nullptr);
      assert((FirstNonempty == nullptr) == (LastNonempty == nullptr));
      last_seg.PrevNonempty = LastNonempty;

      if (LastNonempty) {
        LastNonempty->NextNonempty = &last_seg;
      } else {
        FirstNonempty = &last_seg;
      }

      LastNonempty = &last_seg;
      assert(FirstNonempty);
      assert(LastNonempty);
      assert(FirstNonempty->PrevNonempty == nullptr);
      assert(LastNonempty->NextNonempty == nullptr);
      assert((NumSegments > 1) || (FirstNonempty == LastNonempty));
    }

    std::list<TSegment> AllSegments;

    /* Maintain separate segment count, since AllSegments.size() is not O(1).
     */
    size_t NumSegments;

    /* Total item count for all segments. */
    size_t TotalItems;

    /* When list contains at least one nonempty segment, these point to the
       first and last nonempty segment, respectively.  Together with the
       'PrevNonempty' and 'NextNonempty' members of TSegment, these maintain a
       linked list all nonempty segments, in the same order as they appear in
       'AllSegments'.  The list facilitates an O(1) implementation of
       RemoveOneNewest().  Both pointers are null when all segments are empty.
     */
    TSegment *FirstNonempty;
    TSegment *LastNonempty;
  };  //  TSegmentedList

}  // Thread
