/* <thread/segmented_list.test.cc>
 
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
 
   Unit test for <thread/segmented_list.h>.
 */
  
#include <thread/segmented_list.h>
  
#include <gtest/gtest.h>

using namespace Thread;

namespace {

  /* The fixture for testing class TSegmentedList. */
  class TSegmentedListTest : public ::testing::Test {
    protected:
    TSegmentedListTest() {
    }

    virtual ~TSegmentedListTest() {
    }

    virtual void SetUp() {
    }

    virtual void TearDown() {
    }

    std::list<std::list<int>> EmptyResult() {
      return std::list<std::list<int>>({std::list<int>()});
    }

    std::list<std::list<int>> SingleItemResult(int n) {
      return std::list<std::list<int>>({std::list<int>({n})});
    }

    std::list<int> RemovedNoItem() {
      return std::list<int>();
    }

    std::list<int> RemovedItem(int n) {
      return std::list<int>({n});
    }
  };  // TSegmentedListTest

  TEST_F(TSegmentedListTest, Test1) {
    TSegmentedList<int> slist;

    ASSERT_TRUE(slist.Empty());
    ASSERT_EQ(slist.Size(), 0);
    ASSERT_EQ(slist.SegmentCount(), 1);
    ASSERT_EQ(slist.CopyOutSegments(), EmptyResult());
    ASSERT_TRUE(slist.SanityCheck());
    
    slist.AddNew(1);
    ASSERT_TRUE(slist.SanityCheck());
    ASSERT_FALSE(slist.Empty());
    ASSERT_EQ(slist.Size(), 1);
    ASSERT_EQ(slist.SegmentCount(), 1);
    ASSERT_EQ(slist.CopyOutSegments(), SingleItemResult(1));

    ASSERT_EQ(slist.RemoveOneNewest(), RemovedItem(1));
    ASSERT_TRUE(slist.SanityCheck());
    ASSERT_EQ(slist.CopyOutSegments(), EmptyResult());
    ASSERT_EQ(slist.SegmentCount(), 1);
    ASSERT_EQ(slist.Size(), 0);

    ASSERT_EQ(slist.RemoveOneNewest(), RemovedNoItem());
    ASSERT_TRUE(slist.SanityCheck());
    ASSERT_EQ(slist.CopyOutSegments(), EmptyResult());
    ASSERT_EQ(slist.Size(), 0);

    // only 1 segment, so should not change anything
    slist.RecycleOldestSegment();
    ASSERT_TRUE(slist.SanityCheck());
    ASSERT_EQ(slist.CopyOutSegments(), EmptyResult());
    ASSERT_EQ(slist.SegmentCount(), 1);
    ASSERT_EQ(slist.Size(), 0);

    slist.AddNew(2);
    ASSERT_TRUE(slist.SanityCheck());
    slist.AddNew(3);
    ASSERT_TRUE(slist.SanityCheck());
    std::list<std::list<int>> expected;
    expected.push_back(std::list<int>({3, 2}));
    // should be ((3, 2))
    ASSERT_EQ(slist.CopyOutSegments(), expected);
    ASSERT_EQ(slist.SegmentCount(), 1);
    ASSERT_EQ(slist.Size(), 2);

    // only 1 segment, so should not change anything
    slist.RecycleOldestSegment();
    ASSERT_TRUE(slist.SanityCheck());
    ASSERT_EQ(slist.CopyOutSegments(), expected);
    ASSERT_EQ(slist.SegmentCount(), 1);
    ASSERT_EQ(slist.Size(), 2);

    ASSERT_EQ(slist.RemoveOneNewest(), RemovedItem(3));
    ASSERT_TRUE(slist.SanityCheck());
    // should be ((2))
    ASSERT_EQ(slist.CopyOutSegments(), SingleItemResult(2));
    ASSERT_EQ(slist.SegmentCount(), 1);
    ASSERT_EQ(slist.Size(), 1);

    slist.AddNew(4);
    ASSERT_TRUE(slist.SanityCheck());
    expected.clear();
    expected.push_back(std::list<int>({4, 2}));
    // should be ((4, 2))
    ASSERT_EQ(slist.CopyOutSegments(), expected);
    ASSERT_EQ(slist.SegmentCount(), 1);
    ASSERT_EQ(slist.Size(), 2);

    slist.AddNewSegment();
    ASSERT_TRUE(slist.SanityCheck());
    expected.push_front(std::list<int>());
    // should be ((), (4, 2))
    ASSERT_EQ(slist.CopyOutSegments(), expected);
    ASSERT_EQ(slist.SegmentCount(), 2);
    ASSERT_EQ(slist.Size(), 2);

    slist.AddNew(5);
    ASSERT_TRUE(slist.SanityCheck());
    expected.pop_front();
    expected.push_front(std::list<int>({5}));
    // should be ((5), (4, 2))
    ASSERT_EQ(slist.CopyOutSegments(), expected);
    ASSERT_EQ(slist.SegmentCount(), 2);
    ASSERT_EQ(slist.Size(), 3);

    ASSERT_EQ(slist.RemoveOneNewest(), RemovedItem(5));
    ASSERT_TRUE(slist.SanityCheck());
    expected.pop_front();
    expected.push_front(std::list<int>());
    // should be ((), (4, 2))
    ASSERT_EQ(slist.CopyOutSegments(), expected);
    ASSERT_EQ(slist.SegmentCount(), 2);
    ASSERT_EQ(slist.Size(), 2);

    ASSERT_EQ(slist.RemoveOneNewest(), RemovedItem(4));
    ASSERT_TRUE(slist.SanityCheck());
    expected.pop_back();
    // should be ((), (2))
    expected.push_back(std::list<int>({2}));
    ASSERT_EQ(slist.CopyOutSegments(), expected);
    ASSERT_EQ(slist.SegmentCount(), 2);
    ASSERT_EQ(slist.Size(), 1);

    slist.AddNewSegment();
    ASSERT_TRUE(slist.SanityCheck());
    expected.push_front(std::list<int>());
    // should be ((), (), (2))
    ASSERT_EQ(slist.CopyOutSegments(), expected);
    ASSERT_EQ(slist.SegmentCount(), 3);
    ASSERT_EQ(slist.Size(), 1);

    slist.AddNew(6);
    ASSERT_TRUE(slist.SanityCheck());
    expected.front().push_front(6);
    // should be ((6), (), (2))
    ASSERT_EQ(slist.CopyOutSegments(), expected);
    ASSERT_EQ(slist.SegmentCount(), 3);
    ASSERT_EQ(slist.Size(), 2);

    slist.AddNew(7);
    ASSERT_TRUE(slist.SanityCheck());
    expected.front().push_front(7);
    // should be ((7, 6), (), (2))
    ASSERT_EQ(slist.CopyOutSegments(), expected);
    ASSERT_EQ(slist.SegmentCount(), 3);
    ASSERT_EQ(slist.Size(), 3);

    ASSERT_EQ(slist.RemoveOneNewest(), RemovedItem(7));
    ASSERT_TRUE(slist.SanityCheck());
    // should be ((6), (), (2))
    expected.front().pop_front();
    ASSERT_EQ(slist.CopyOutSegments(), expected);
    ASSERT_EQ(slist.SegmentCount(), 3);
    ASSERT_EQ(slist.Size(), 2);

    ASSERT_EQ(slist.RemoveOneNewest(), RemovedItem(6));
    ASSERT_TRUE(slist.SanityCheck());
    // should be ((), (), (2))
    expected.front().pop_front();
    ASSERT_EQ(slist.CopyOutSegments(), expected);
    ASSERT_EQ(slist.SegmentCount(), 3);
    ASSERT_EQ(slist.Size(), 1);

    ASSERT_EQ(slist.RemoveOneNewest(), RemovedItem(2));
    ASSERT_TRUE(slist.SanityCheck());
    // should be ((), (), ())
    expected.back().pop_front();
    ASSERT_EQ(slist.CopyOutSegments(), expected);
    ASSERT_EQ(slist.SegmentCount(), 3);
    ASSERT_EQ(slist.Size(), 0);

    ASSERT_EQ(slist.RemoveOneNewest(), RemovedNoItem());
    ASSERT_TRUE(slist.SanityCheck());
    // should be ((), (), ())
    ASSERT_EQ(slist.CopyOutSegments(), expected);
    ASSERT_EQ(slist.SegmentCount(), 3);
    ASSERT_EQ(slist.Size(), 0);

    slist.ResetSegments();
    ASSERT_TRUE(slist.SanityCheck());
    // should be (())
    ASSERT_EQ(slist.CopyOutSegments(), EmptyResult());
    ASSERT_EQ(slist.SegmentCount(), 1);
    ASSERT_EQ(slist.Size(), 0);

    slist.AddNew(8);
    ASSERT_TRUE(slist.SanityCheck());
    slist.AddNew(9);
    ASSERT_TRUE(slist.SanityCheck());
    slist.AddNewSegment();
    ASSERT_TRUE(slist.SanityCheck());
    slist.AddNewSegment();
    ASSERT_TRUE(slist.SanityCheck());
    slist.AddNew(10);
    ASSERT_TRUE(slist.SanityCheck());
    slist.AddNewSegment();
    ASSERT_TRUE(slist.SanityCheck());
    slist.AddNewSegment();
    ASSERT_TRUE(slist.SanityCheck());
    slist.AddNew(11);
    ASSERT_TRUE(slist.SanityCheck());
    slist.AddNew(12);
    ASSERT_TRUE(slist.SanityCheck());
    slist.AddNewSegment();
    ASSERT_TRUE(slist.SanityCheck());
    slist.AddNew(13);
    ASSERT_TRUE(slist.SanityCheck());
    slist.AddNew(14);
    ASSERT_TRUE(slist.SanityCheck());
    slist.AddNew(15);
    ASSERT_TRUE(slist.SanityCheck());
    slist.AddNewSegment();
    ASSERT_TRUE(slist.SanityCheck());
    slist.AddNewSegment();
    ASSERT_TRUE(slist.SanityCheck());
    slist.AddNew(16);
    ASSERT_TRUE(slist.SanityCheck());
    slist.AddNew(17);
    ASSERT_TRUE(slist.SanityCheck());

    expected.clear();
    expected.push_front(std::list<int>({9, 8}));
    expected.push_front(std::list<int>());
    expected.push_front(std::list<int>({10}));
    expected.push_front(std::list<int>());
    expected.push_front(std::list<int>({12, 11}));
    expected.push_front(std::list<int>({15, 14, 13}));
    expected.push_front(std::list<int>());
    expected.push_front(std::list<int>({17, 16}));
    // should be ((17, 16), (), (15, 14, 13), (12, 11), (), (10), (), (9, 8))
    ASSERT_EQ(slist.CopyOutSegments(), expected);
    ASSERT_EQ(slist.SegmentCount(), 8);
    ASSERT_EQ(slist.Size(), 10);

    slist.RecycleOldestSegment();
    ASSERT_TRUE(slist.SanityCheck());
    expected.push_front(std::list<int>());
    expected.pop_back();
    expected.pop_back();
    expected.push_back(std::list<int>({9, 8}));
    // should be ((), (17, 16), (), (15, 14, 13), (12, 11), (), (10), (9, 8))
    ASSERT_EQ(slist.CopyOutSegments(), expected);
    ASSERT_EQ(slist.SegmentCount(), 8);
    ASSERT_EQ(slist.Size(), 10);

    slist.RecycleOldestSegment();
    ASSERT_TRUE(slist.SanityCheck());
    expected.push_front(std::list<int>());
    expected.pop_back();
    expected.pop_back();
    // should be ((), (), (17, 16), (), (15, 14, 13), (12, 11), (), (10, 9, 8))
    expected.push_back(std::list<int>({10, 9, 8}));
    ASSERT_EQ(slist.CopyOutSegments(), expected);
    ASSERT_EQ(slist.SegmentCount(), 8);
    ASSERT_EQ(slist.Size(), 10);

    ASSERT_EQ(slist.RemoveOldest(1), RemovedItem(8));
    ASSERT_TRUE(slist.SanityCheck());
    expected.back().pop_back();
    // should be ((), (), (17, 16), (), (15, 14, 13), (12, 11), (), (10, 9))
    ASSERT_EQ(slist.CopyOutSegments(), expected);
    ASSERT_EQ(slist.SegmentCount(), 8);
    ASSERT_EQ(slist.Size(), 9);

    std::list<int> items = slist.RemoveOldest(3);
    ASSERT_TRUE(slist.SanityCheck());
    ASSERT_EQ(items, std::list<int>({10, 9}));
    expected.back().clear();
    // should be ((), (), (17, 16), (), (15, 14, 13), (12, 11), (), ())
    ASSERT_EQ(slist.CopyOutSegments(), expected);
    ASSERT_EQ(slist.SegmentCount(), 8);
    ASSERT_EQ(slist.Size(), 7);

    ASSERT_EQ(slist.RemoveOldest(1), RemovedNoItem());
    ASSERT_TRUE(slist.SanityCheck());
    // should be ((), (), (17, 16), (), (15, 14, 13), (12, 11), (), ())
    ASSERT_EQ(slist.CopyOutSegments(), expected);
    ASSERT_EQ(slist.SegmentCount(), 8);
    ASSERT_EQ(slist.Size(), 7);

    ASSERT_EQ(slist.EmptyOldest(), RemovedNoItem());
    ASSERT_TRUE(slist.SanityCheck());
    // should be ((), (), (17, 16), (), (15, 14, 13), (12, 11), (), ())
    ASSERT_EQ(slist.CopyOutSegments(), expected);
    ASSERT_EQ(slist.SegmentCount(), 8);
    ASSERT_EQ(slist.Size(), 7);

    slist.RecycleOldestSegment();
    ASSERT_TRUE(slist.SanityCheck());
    expected.pop_back();
    expected.push_front(std::list<int>());
    // should be ((), (), (), (17, 16), (), (15, 14, 13), (12, 11), ())
    ASSERT_EQ(slist.CopyOutSegments(), expected);
    ASSERT_EQ(slist.SegmentCount(), 8);
    ASSERT_EQ(slist.Size(), 7);

    slist.RecycleOldestSegment();
    ASSERT_TRUE(slist.SanityCheck());
    expected.pop_back();
    expected.push_front(std::list<int>());
    // should be ((), (), (), (), (17, 16), (), (15, 14, 13), (12, 11))
    ASSERT_EQ(slist.CopyOutSegments(), expected);
    ASSERT_EQ(slist.SegmentCount(), 8);
    ASSERT_EQ(slist.Size(), 7);

    items = slist.EmptyOldest();
    ASSERT_TRUE(slist.SanityCheck());
    ASSERT_EQ(items, std::list<int>({12, 11}));
    expected.back().clear();
    // should be ((), (), (), (), (17, 16), (), (15, 14, 13), ())
    ASSERT_EQ(slist.CopyOutSegments(), expected);
    ASSERT_EQ(slist.SegmentCount(), 8);
    ASSERT_EQ(slist.Size(), 5);

    slist.AddNew(18);
    ASSERT_TRUE(slist.SanityCheck());
    expected.front().push_front(18);
    // should be ((18), (), (), (), (17, 16), (), (15, 14, 13), ())
    ASSERT_EQ(slist.CopyOutSegments(), expected);
    ASSERT_EQ(slist.SegmentCount(), 8);
    ASSERT_EQ(slist.Size(), 6);

    slist.ResetSegments();
    ASSERT_TRUE(slist.SanityCheck());
    expected.clear();
    expected.push_back(std::list<int>({18, 17, 16, 15, 14, 13}));
    // should be ((18, 17, 16, 15, 14, 13))
    ASSERT_EQ(slist.CopyOutSegments(), expected);
    ASSERT_EQ(slist.SegmentCount(), 1);
    ASSERT_EQ(slist.Size(), 6);

    ASSERT_EQ(slist.RemoveOneNewest(), RemovedItem(18));
    ASSERT_TRUE(slist.SanityCheck());
    // should be ((17, 16, 15, 14, 13))
    expected.front().pop_front();
    ASSERT_EQ(slist.CopyOutSegments(), expected);
    ASSERT_EQ(slist.SegmentCount(), 1);
    ASSERT_EQ(slist.Size(), 5);

    items = slist.RemoveOldest(2);
    ASSERT_TRUE(slist.SanityCheck());
    ASSERT_EQ(items, std::list<int>({14, 13}));
    expected.back().pop_back();
    expected.back().pop_back();
    // should be ((17, 16, 15))
    ASSERT_EQ(slist.CopyOutSegments(), expected);
    ASSERT_EQ(slist.SegmentCount(), 1);
    ASSERT_EQ(slist.Size(), 3);

    items = slist.RemoveOldest(3);
    ASSERT_TRUE(slist.SanityCheck());
    ASSERT_EQ(items, std::list<int>({17, 16, 15}));
    expected.clear();
    expected.push_back(std::list<int>());
    // should be (())
    ASSERT_EQ(slist.CopyOutSegments(), expected);
    ASSERT_EQ(slist.SegmentCount(), 1);
    ASSERT_EQ(slist.Size(), 0);

    slist.AddNew(19);
    ASSERT_TRUE(slist.SanityCheck());
    slist.AddNew(20);
    ASSERT_TRUE(slist.SanityCheck());
    slist.AddNewSegment();
    ASSERT_TRUE(slist.SanityCheck());
    slist.AddNewSegment();
    ASSERT_TRUE(slist.SanityCheck());
    slist.AddNew(21);
    ASSERT_TRUE(slist.SanityCheck());
    expected.front().push_front(19);
    expected.front().push_front(20);
    expected.push_front(std::list<int>());
    expected.push_front(std::list<int>());
    expected.front().push_front(21);
    // should be ((21), (), (20, 19))
    ASSERT_EQ(slist.CopyOutSegments(), expected);
    ASSERT_EQ(slist.SegmentCount(), 3);
    ASSERT_EQ(slist.Size(), 3);

    items = slist.EmptyAll();
    ASSERT_TRUE(slist.SanityCheck());
    ASSERT_EQ(items, std::list<int>({21, 20, 19}));
    ASSERT_TRUE(slist.Empty());
    ASSERT_EQ(slist.SegmentCount(), 3);
    ASSERT_EQ(slist.RemoveOneNewest(), RemovedNoItem());
    ASSERT_TRUE(slist.Empty());
    ASSERT_EQ(slist.SegmentCount(), 3);

    slist.ResetSegments();
    ASSERT_TRUE(slist.SanityCheck());
    expected.clear();
    expected.push_back(std::list<int>());
    // should be (())
    ASSERT_EQ(slist.CopyOutSegments(), expected);
    ASSERT_EQ(slist.SegmentCount(), 1);
    ASSERT_EQ(slist.Size(), 0);
    ASSERT_EQ(slist.RemoveOneNewest(), RemovedNoItem());
    ASSERT_EQ(slist.SegmentCount(), 1);
    ASSERT_EQ(slist.Size(), 0);

    slist.AddNew(22);
    ASSERT_TRUE(slist.SanityCheck());
    slist.AddNew(23);
    ASSERT_TRUE(slist.SanityCheck());
    slist.AddNewSegment();
    ASSERT_TRUE(slist.SanityCheck());
    slist.AddNewSegment();
    ASSERT_TRUE(slist.SanityCheck());
    slist.AddNew(24);
    ASSERT_TRUE(slist.SanityCheck());
    expected.front().push_front(22);
    ASSERT_TRUE(slist.SanityCheck());
    expected.front().push_front(23);
    ASSERT_TRUE(slist.SanityCheck());
    expected.push_front(std::list<int>());
    expected.push_front(std::list<int>());
    expected.front().push_front(24);
    // should be ((24), (), (23, 22))
    ASSERT_EQ(slist.CopyOutSegments(), expected);
    ASSERT_EQ(slist.SegmentCount(), 3);
    ASSERT_EQ(slist.Size(), 3);

    items = slist.EmptyAllAndResetSegments();
    ASSERT_TRUE(slist.SanityCheck());
    ASSERT_EQ(items, std::list<int>({24, 23, 22}));
    ASSERT_TRUE(slist.Empty());
    ASSERT_EQ(slist.SegmentCount(), 1);
    ASSERT_EQ(slist.RemoveOneNewest(), RemovedNoItem());
    ASSERT_TRUE(slist.Empty());
    ASSERT_EQ(slist.SegmentCount(), 1);
  }

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
