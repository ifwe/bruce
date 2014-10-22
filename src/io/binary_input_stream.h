/* <io/binary_input_stream.h>

   ----------------------------------------------------------------------------
   Copyright 2010-2013 if(we)

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

   An input stream in binary format.
 */

#pragma once

#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <list>
#include <map>
#include <set>
#include <string>
#include <tuple>
#include <vector>
#include <unordered_map>
#include <unordered_set>

#include <base/no_copy_semantics.h>
#include <base/no_construction.h>
#include <io/binary_stream.h>
#include <io/input_consumer.h>

namespace Io {

  /* An input stream in binary format. */
  class TBinaryInputStream : public virtual TBinaryStream,
                             public TInputConsumer {
    NO_COPY_SEMANTICS(TBinaryInputStream);

    public:
    using TInputConsumer::ReadExactly;

    /* If someone wants to be able to consume this data in chunks without
       copying it. */
    using TInputConsumer::GetPeekSize;
    using TInputConsumer::Peek;
    using TInputConsumer::SkipExactly;

    /* Read built-in types. */

    void Read(bool &that) {
      ReadWithoutSwap(that);
    }

    void Read(char &that) {
      ReadWithoutSwap(that);
    }

    void Read(float &that) {
      ReadWithoutSwap(that);
    }

    void Read(double &that) {
      ReadWithoutSwap(that);
    }

    void Read(int8_t &that) {
      ReadWithoutSwap(that);
    }

    void Read(int16_t &that) {
      ReadWithSwap(that);
    }

    void Read(int32_t &that) {
      ReadWithSwap(that);
    }

    void Read(int64_t &that) {
      ReadWithSwap(that);
    }

    void Read(uint8_t &that) {
      ReadWithoutSwap(that);
    }

    void Read(uint16_t &that) {
      ReadWithSwap(that);
    }

    void Read(uint32_t &that) {
      ReadWithSwap(that);
    }

    void Read(uint64_t &that) {
      ReadWithSwap(that);
    }

    /* Read STL containers. */

    template <typename TVal, typename TAlloc>
    void Read(std::list<TVal, TAlloc> &that) {
      ReadPushableContainer(that);
    }

    template <typename TKey, typename TVal, typename TCompare, typename TAlloc>
    void Read(std::map<TKey, TVal, TCompare, TAlloc> &that) {
      ReadInsertableContainer(that);
    }

    template <typename TKey, typename TVal, typename TCompare, typename TAlloc>
    void Read(std::multimap<TKey, TVal, TCompare, TAlloc> &that) {
      ReadInsertableContainer(that);
    }

    template <typename TVal, typename TCompare, typename TAlloc>
    void Read(std::multiset<TVal, TCompare, TAlloc> &that) {
      ReadInsertableContainer(that);
    }

    template <typename TFirst, typename TSecond>
    void Read(std::pair<TFirst, TSecond> &that) {
      /* Read(that.first); Read(that.second); */
      *this >> that.first >> that.second;
    }

    template <typename TVal, typename TCompare, typename TAlloc>
    void Read(std::set<TVal, TCompare, TAlloc> &that) {
      ReadInsertableContainer(that);
    }

    template <typename TKey, typename TVal, typename THash, typename TEq,
              typename TAlloc>
    void Read(std::unordered_map<TKey, TVal, THash, TEq, TAlloc> &that) {
      ReadInsertableContainer(that);
    }

    template <typename TKey, typename TVal, typename THash, typename TEq,
              typename TAlloc>
    void Read(std::unordered_multimap<TKey, TVal, THash, TEq, TAlloc> &that) {
      ReadInsertableContainer(that);
    }

    template <typename TVal, typename THash, typename TEq, typename TAlloc>
    void Read(std::unordered_set<TVal, THash, TEq, TAlloc> &that) {
      ReadInsertableContainer(that);
    }

    template <typename TVal, typename THash, typename TEq, typename TAlloc>
    void Read(std::unordered_multiset<TVal, THash, TEq, TAlloc> &that) {
      ReadInsertableContainer(that);
    }

    template <typename TVal, typename TAlloc>
    void Read(std::vector<TVal, TAlloc> &that) {
      ReadPushableContainer(that);
    }

    /* Read a string. */
    void Read(std::string &that) {
      assert(this);
      assert(&that);
      size_t size;
      Read(size);
      that.resize(size);
      ReadExactly(const_cast<char *>(that.data()), size);
    }

    /* Read STL tuples. */

    void Read(std::tuple<> &) {
    }

    template <typename... TArgs>
    void Read(std::tuple<TArgs...> &that) {
      assert(this);
      assert(&that);
      TTupleReader<0, TArgs...>::Read(this, that);
    }

    protected:
    /* Attach to the given producer, which must not be null. */
    TBinaryInputStream(const std::shared_ptr<TInputProducer> &input_producer)
        : TInputConsumer(input_producer) {
    }

    private:
    template <typename TVal>
    class TypeHelper;

    /* A helper class for reading tuples.
       NOTE: We're invading the implementation of std::tuple here because the
       API doesn't support head/tail abstractions.  If this bothers you, please
       feel free to beat your head against the brick wall of the ISO committee.
     */
    template <size_t N, typename... TArgs>
    class TTupleReader;

    /* Read an STL container that supports insert(). */
    template <typename TThat>
    void ReadInsertableContainer(TThat &that);

    /* Read an STL container that supports push_back(). */
    template <typename TThat>
    void ReadPushableContainer(TThat &that) {
      assert(this);
      assert(&that);
      size_t size;
      Read(size);
      that.clear();
      typename TThat::value_type val;

      for (size_t i = 0; i < size; ++i) {
        *this >> val;
        //Read(val);
        that.push_back(val);
      }
    }

    /* Read a built-in, converting from NBO if necessary. */
    template <typename TThat>
    void ReadWithSwap(TThat &that) {
      assert(this);
      assert(&that);
      ReadExactly(&that, sizeof(that));
      GetFormat().ConvertInt(that);
    }

    /* Read a built-in without converting from NBO. */
    template <typename TThat>
    void ReadWithoutSwap(TThat &that) {
      assert(this);
      assert(&that);
      ReadExactly(&that, sizeof(that));
    }

  };  // TBinaryInputStream

  template <typename TVal>
  class TBinaryInputStream::TypeHelper {
    NO_CONSTRUCTION(TypeHelper);

    public:
    typedef TVal TType;
  };  // TypeHelper

  template <typename TFirst, typename TSecond>
  class TBinaryInputStream::TypeHelper<std::pair<const TFirst, TSecond>> {
    NO_CONSTRUCTION(TypeHelper);

    public:
    typedef std::pair<TFirst, TSecond> TType;
  };  // TypeHelper

  template <typename TThat>
  void TBinaryInputStream::ReadInsertableContainer(TThat &that) {
    assert(this);
    assert(&that);
    size_t size;
    Read(size);
    that.clear();
    //typename TThat::value_type val;
    typename TypeHelper<typename TThat::value_type>::TType val;

    for (size_t i = 0; i < size; ++i) {
      *this >> val;
      //Read(val);
      that.insert(val);
    }
  }

  /* Specialization for empty tuples. */
  template <size_t N>
  class TBinaryInputStream::TTupleReader<N> {
    NO_CONSTRUCTION(TTupleReader);

    public:
    /* Read the tuple. */
    static void Read(TBinaryInputStream *, std::_Tuple_impl<N> &) {
    }
  };  // TBinaryInputStream::TTupleReader<N>

  /* Specialization for non-empty tuples. */
  template <size_t N, typename THead, typename... TRest>
  class TBinaryInputStream::TTupleReader<N, THead, TRest...> {
    NO_CONSTRUCTION(TTupleReader);

    public:
    /* Read the tuple. */
    static void Read(TBinaryInputStream *strm,
        std::_Tuple_impl<N, THead, TRest...> &that) {
      *strm >> std::_Tuple_impl<N, THead, TRest...>::_M_head(that);
      // strm->Read(std::_Tuple_impl<N, THead, TRest...>::_M_head(that));
      TTupleReader<N + 1, TRest...>::Read(strm,
          std::_Tuple_impl<N, THead, TRest...>::_M_tail(that));
    }
  };  // TBinaryInputStream::TTupleReader<N>

  /* Used to stream in enumerated types. */
  template <typename TSomeEnum, typename TSomeInt = int32_t>
  class TBinaryInputEnum {
    public:
    /* Do-little. */
    TBinaryInputEnum(TSomeEnum &some_enum)
        : SomeEnum(some_enum) {
    }

    /* Stream in. */
    void Read(TBinaryInputStream &strm) {
      assert(this);
      assert(&strm);
      TSomeInt some_int;
      strm >> some_int;
      SomeEnum = static_cast<TSomeEnum>(some_int);
    }

    private:
    /* The enum into which we read. */
    TSomeEnum &SomeEnum;
  };  // TBinaryInputEnum<TSomeEnum, TSomeInt>

  /* Stream extractors for built-in types. */

  inline TBinaryInputStream &operator>>(TBinaryInputStream &strm, bool &that) {
    strm.Read(that); return strm;
  }

  inline TBinaryInputStream &operator>>(TBinaryInputStream &strm, char &that) {
    strm.Read(that);
    return strm;
  }

  inline TBinaryInputStream &operator>>(TBinaryInputStream &strm,
      float &that) {
    strm.Read(that);
    return strm;
  }

  inline TBinaryInputStream &operator>>(TBinaryInputStream &strm,
      double &that) {
    strm.Read(that);
    return strm;
  }

  inline TBinaryInputStream &operator>>(TBinaryInputStream &strm,
      int8_t &that) {
    strm.Read(that);
    return strm;
  }

  inline TBinaryInputStream &operator>>(TBinaryInputStream &strm,
      int16_t &that) {
    strm.Read(that);
    return strm;
  }

  inline TBinaryInputStream &operator>>(TBinaryInputStream &strm,
      int32_t &that) {
    strm.Read(that);
    return strm;
  }

  inline TBinaryInputStream &operator>>(TBinaryInputStream &strm,
      int64_t &that) {
    strm.Read(that);
    return strm;
  }

  inline TBinaryInputStream &operator>>(TBinaryInputStream &strm,
      uint8_t &that) {
    strm.Read(that);
    return strm;
  }

  inline TBinaryInputStream &operator>>(TBinaryInputStream &strm,
      uint16_t &that) {
    strm.Read(that);
    return strm;
  }

  inline TBinaryInputStream &operator>>(TBinaryInputStream &strm,
      uint32_t &that) {
    strm.Read(that);
    return strm;
  }

  inline TBinaryInputStream &operator>>(TBinaryInputStream &strm,
      uint64_t &that) {
    strm.Read(that);
    return strm;
  }

  /* Stream extractor for strings. */
  inline TBinaryInputStream &operator>>(TBinaryInputStream &strm,
      std::string &that) {
    strm.Read(that);
    return strm;
  }

  /* Stream extractors for STL containers. */

  template <typename TVal, typename TAlloc>
  inline TBinaryInputStream &operator>>(TBinaryInputStream &strm,
      std::list<TVal, TAlloc> &that) {
    strm.Read(that);
    return strm;
  }

  template <typename TKey, typename TVal, typename TCompare, typename TAlloc>
  inline TBinaryInputStream &operator>>(TBinaryInputStream &strm,
      std::map<TKey, TVal, TCompare, TAlloc> &that) {
    strm.Read(that);
    return strm;
  }

  template <typename TKey, typename TVal, typename TCompare, typename TAlloc>
  inline TBinaryInputStream &operator>>(TBinaryInputStream &strm,
      std::multimap<TKey, TVal, TCompare, TAlloc> &that) {
    strm.Read(that);
    return strm;
  }

  template <typename TVal, typename TCompare, typename TAlloc>
  inline TBinaryInputStream &operator>>(TBinaryInputStream &strm,
      std::multiset<TVal, TCompare, TAlloc> &that) {
    strm.Read(that);
    return strm;
  }

  template <typename TFirst, typename TSecond>
  inline TBinaryInputStream &operator>>(TBinaryInputStream &strm,
      std::pair<TFirst, TSecond> &that) {
    strm.Read(that);
    return strm;
  }

  template <typename TVal, typename TCompare, typename TAlloc>
  inline TBinaryInputStream &operator>>(TBinaryInputStream &strm,
      std::set<TVal, TCompare, TAlloc> &that) {
    strm.Read(that);
    return strm;
  }

  template <typename TKey, typename TVal, typename THash, typename TEq,
            typename TAlloc>
  inline TBinaryInputStream &operator>>(TBinaryInputStream &strm,
      std::unordered_map<TKey, TVal, THash, TEq, TAlloc> &that) {
    strm.Read(that);
    return strm;
  }

  template <typename TKey, typename TVal, typename THash, typename TEq,
            typename TAlloc>
  inline TBinaryInputStream &operator>>(TBinaryInputStream &strm,
      std::unordered_multimap<TKey, TVal, THash, TEq, TAlloc> &that) {
    strm.Read(that);
    return strm;
  }

  template <typename TVal, typename THash, typename TEq, typename TAlloc>
  inline TBinaryInputStream &operator>>(TBinaryInputStream &strm,
      std::unordered_set<TVal, THash, TEq, TAlloc> &that) {
    strm.Read(that);
    return strm;
  }

  template <typename TVal, typename THash, typename TEq, typename TAlloc>
  inline TBinaryInputStream &operator>>(TBinaryInputStream &strm,
      std::unordered_multiset<TVal, THash, TEq, TAlloc> &that) {
    strm.Read(that);
    return strm;
  }

  template <typename TVal, typename TAlloc>
  inline TBinaryInputStream &operator>>(TBinaryInputStream &strm,
      std::vector<TVal, TAlloc> &that) {
    strm.Read(that);
    return strm;
  }

  /* Stream extractor for STL tuple. */
  template <typename... TArgs>
  inline TBinaryInputStream &operator>>(TBinaryInputStream &strm,
      std::tuple<TArgs...> &that) {
    strm.Read(that);
    return strm;
  }

  /* Stream extractor for std::chrono. */
  template <typename TRep, typename TPeriod>
  inline TBinaryInputStream &operator>>(TBinaryInputStream &strm,
      std::chrono::duration<TRep, TPeriod> &that) {
    TRep rep;
    strm >> rep;
    that = std::chrono::duration<TRep, TPeriod>(rep);
    return strm;
  }

  /* Stream extractors for enums. */

  template <typename TSomeEnum, typename TSomeInt>
  inline TBinaryInputStream &operator>>(TBinaryInputStream &strm,
      TBinaryInputEnum<TSomeEnum, TSomeInt> &that) {
    assert(&that);
    that.Read(strm);
    return strm;
  }

  template <typename TSomeEnum, typename TSomeInt>
  inline TBinaryInputStream &operator>>(TBinaryInputStream &strm,
      TBinaryInputEnum<TSomeEnum, TSomeInt> &&that) {
    assert(&that);
    that.Read(strm);
    return strm;
  }

  /* Stream extractors for built-in types. */

  inline TBinaryInputStream &&operator>>(TBinaryInputStream &&strm,
      bool &that) {
    strm.Read(that); return std::move(strm);
  }

  inline TBinaryInputStream &&operator>>(TBinaryInputStream &&strm,
      char &that) {
    strm.Read(that); return std::move(strm);
  }

  inline TBinaryInputStream &&operator>>(TBinaryInputStream &&strm,
      float &that) {
    strm.Read(that); return std::move(strm);
  }

  inline TBinaryInputStream &&operator>>(TBinaryInputStream &&strm,
      double &that) {
    strm.Read(that); return std::move(strm);
  }

  inline TBinaryInputStream &&operator>>(TBinaryInputStream &&strm,
      int8_t &that) {
    strm.Read(that); return std::move(strm);
  }

  inline TBinaryInputStream &&operator>>(TBinaryInputStream &&strm,
      int16_t &that) {
    strm.Read(that); return std::move(strm);
  }

  inline TBinaryInputStream &&operator>>(TBinaryInputStream &&strm,
      int32_t &that) {
    strm.Read(that); return std::move(strm);
  }

  inline TBinaryInputStream &&operator>>(TBinaryInputStream &&strm,
      int64_t &that) {
    strm.Read(that); return std::move(strm);
  }

  inline TBinaryInputStream &&operator>>(TBinaryInputStream &&strm,
      uint8_t &that) {
    strm.Read(that); return std::move(strm);
  }

  inline TBinaryInputStream &&operator>>(TBinaryInputStream &&strm,
      uint16_t &that) {
    strm.Read(that); return std::move(strm);
  }

  inline TBinaryInputStream &&operator>>(TBinaryInputStream &&strm,
      uint32_t &that) {
    strm.Read(that); return std::move(strm);
  }

  inline TBinaryInputStream &&operator>>(TBinaryInputStream &&strm,
      uint64_t &that) {
    strm.Read(that); return std::move(strm);
  }

  /* Stream extractor for strings. */
  inline TBinaryInputStream &&operator>>(TBinaryInputStream &&strm,
      std::string &that) {
    strm.Read(that);
    return std::move(strm);
  }

  /* Stream extractors for STL containers. */

  template <typename TVal, typename TAlloc>
  inline TBinaryInputStream &&operator>>(TBinaryInputStream &&strm,
      std::list<TVal, TAlloc> &that) {
    strm.Read(that);
    return std::move(strm);
  }

  template <typename TKey, typename TVal, typename TCompare, typename TAlloc>
  inline TBinaryInputStream &&operator>>(TBinaryInputStream &&strm,
      std::map<TKey, TVal, TCompare, TAlloc> &that) {
    strm.Read(that);
    return std::move(strm);
  }

  template <typename TKey, typename TVal, typename TCompare, typename TAlloc>
  inline TBinaryInputStream &&operator>>(TBinaryInputStream &&strm,
      std::multimap<TKey, TVal, TCompare, TAlloc> &that) {
    strm.Read(that);
    return std::move(strm);
  }

  template <typename TVal, typename TCompare, typename TAlloc>
  inline TBinaryInputStream &&operator>>(TBinaryInputStream &&strm,
      std::multiset<TVal, TCompare, TAlloc> &that) {
    strm.Read(that);
    return std::move(strm);
  }

  template <typename TFirst, typename TSecond>
  inline TBinaryInputStream &&operator>>(TBinaryInputStream &&strm,
      std::pair<TFirst, TSecond> &that) {
    strm.Read(that);
    return std::move(strm);
  }

  template <typename TVal, typename TCompare, typename TAlloc>
  inline TBinaryInputStream &&operator>>(TBinaryInputStream &&strm,
      std::set<TVal, TCompare, TAlloc> &that) {
    strm.Read(that);
    return std::move(strm);
  }

  template <typename TKey, typename TVal, typename THash, typename TEq,
            typename TAlloc>
  inline TBinaryInputStream &&operator>>(TBinaryInputStream &&strm,
      std::unordered_map<TKey, TVal, THash, TEq, TAlloc> &that) {
    strm.Read(that);
    return std::move(strm);
  }

  template <typename TKey, typename TVal, typename THash, typename TEq,
            typename TAlloc>
  inline TBinaryInputStream &&operator>>(TBinaryInputStream &&strm,
      std::unordered_multimap<TKey, TVal, THash, TEq, TAlloc> &that) {
    strm.Read(that);
    return std::move(strm);
  }

  template <typename TVal, typename THash, typename TEq, typename TAlloc>
  inline TBinaryInputStream &&operator>>(TBinaryInputStream &&strm,
      std::unordered_set<TVal, THash, TEq, TAlloc> &that) {
    strm.Read(that);
    return std::move(strm);
  }

  template <typename TVal, typename THash, typename TEq, typename TAlloc>
  inline TBinaryInputStream &&operator>>(TBinaryInputStream &&strm,
      std::unordered_multiset<TVal, THash, TEq, TAlloc> &that) {
    strm.Read(that);
    return std::move(strm);
  }

  template <typename TVal, typename TAlloc>
  inline TBinaryInputStream &&operator>>(TBinaryInputStream &&strm,
      std::vector<TVal, TAlloc> &that) {
    strm.Read(that);
    return std::move(strm);
  }

  /* Stream extractor for STL tuple. */
  template <typename... TArgs>
  inline TBinaryInputStream &&operator>>(TBinaryInputStream &&strm,
      std::tuple<TArgs...> &that) {
    strm.Read(that);
    return std::move(strm);
  }

}  // Io
