/* <base/stl_utils.h>

   ----------------------------------------------------------------------------
   Copyright 2013 if(we)

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

   Some utilities for working with STL containers.
 */

#pragma once

#include <initializer_list>
#include <limits>
#include <syslog.h>

#include <base/impossible_error.h>
#include <server/daemonize.h>

namespace Base {

  /* Return true iff. a given value is in a container. */
  template <typename TContainer>
  bool Contains(const TContainer &container,
      const typename TContainer::value_type &val) {
    return container.find(val) != container.end();
  }

  /* Return true iff. a given key is in an associative container. */
  template <typename TContainer>
  bool ContainsKey(const TContainer &container,
      const typename TContainer::key_type &key) {
    return container.find(key) != container.end();
  }

  /* Deletes each key in an associative container. */
  template <typename TContainer>
  void DeleteEachKey(TContainer &container) {
    for (auto iter = container.begin(); iter != container.end(); ++iter) {
      delete iter->first;
    }
  }

  /* Deletes each key and value in an associative container. */
  template <typename TContainer>
  void DeleteEachKeyAndValue(TContainer &container) {
    for (auto iter = container.begin(); iter != container.end(); ++iter) {
      delete iter->first;
      delete iter->second;
    }
  }

  /* Deletes each value in an associative container. */
  template <typename TContainer>
  void DeleteEachValue(TContainer &container) {
    for (auto iter = container.begin(); iter != container.end(); ++iter) {
      delete iter->second;
    }
  }

  /* Erase the given value from the container.  If the value is not in the
     container, fail an assertion. */
  template <typename TContainer>
  void EraseOrFail(TContainer &container,
      const typename TContainer::key_type &key) {
    auto result = container.erase(key);
    assert(result == 1);
  }

  /* Returns the value mapped to the given key.  If the value doesn't appear in
     the container, an assertion fails. */
  template <typename TContainer>
  const typename TContainer::mapped_type &Find(const TContainer &container,
      const typename TContainer::key_type &key) {
    auto iter = container.find(key);
    assert(iter != container.end());
    return iter->second;
  }

  /* Returns the value mapped to the given key.  If the value doesn't appear in
     the container, returns a default. */
  template <typename TContainer>
  const typename TContainer::mapped_type &FindOrDefault(
      const TContainer &container, const typename TContainer::key_type &key,
      const typename TContainer::mapped_type &def) {
    auto iter = container.find(key);
    return (iter != container.end()) ? iter->second : def;
  }

  /* Returns the value mapped to the given key.  If the value doesn't appear in
     the container, an assertion fails. */
  template <typename TContainer>
  const typename TContainer::mapped_type &FindOrElse(
      const TContainer &container, const typename TContainer::key_type &key) {
    auto iter = container.find(key);

    if (iter == container.end()) {
      throw TImpossibleError(HERE);
    }

    assert(iter != container.end());
    return iter->second;
  }

  /* Returns the value mapped to the given key.  If the value doesn't appear in
     the container, the default value is inserted and returned. */
  template <typename TContainer>
  const typename TContainer::mapped_type &FindOrInsert(
      TContainer &container, const typename TContainer::key_type &key,
      const typename TContainer::mapped_type &def) {
    return container.insert(
        typename TContainer::value_type(key, def)).first->second;
  }

  /* Insert the given value into the container.  If the value is already in the
     container, fail an assertion. */
  template <typename TContainer>
  void InsertOrFail(TContainer &container,
      const typename TContainer::value_type &value) {
    auto result = container.insert(value);

    if (!result.second) {
      syslog(LOG_ERR, "[InsertOrFail]");
      Server::BacktraceToLog();
    }

    assert(result.second);
  }

  /* Inserts the pointer-type value into the associative container under the
     given key.  If the container already contains a value for the key,
     the old value is deleted and replaced with the new one. */
  template <typename TContainer>
  void InsertOrReplacePtr(TContainer &container,
      const typename TContainer::key_type &key,
      const typename TContainer::mapped_type &new_value) {
    auto result = container.insert(
        typename TContainer::value_type(key, new_value));
    typename TContainer::mapped_type &old_value = (result.first)->second;

    if (old_value != new_value) {
      delete old_value;
      old_value = new_value;
    }
  }

  /* Returns an integer rotated to the left by n bits. */
  template <typename TVal>
  TVal RotatedLeft(TVal val, int n) {
    const int digits = std::numeric_limits<TVal>::digits;
    n %= digits;
    return (val << n) | (val >> (digits - n));
  }

  /* Returns an integer rotated to the right by n bits. */
  template <typename TVal>
  TVal RotatedRight(TVal val, int n) {
    const int digits = std::numeric_limits<TVal>::digits;
    n %= digits;
    return (val >> n) | (val << (digits - n));
  }

  /* Returns a pointer to the value mapped to the given key.  If the value
     doesn't appear in the container, return a null pointer. */
  template <typename TContainer>
  const typename TContainer::mapped_type *TryFind(const TContainer &container,
      const typename TContainer::key_type &key) {
    auto iter = container.find(key);
    return (iter != container.end()) ? &(iter->second) : 0;
  }

  /* Returns true if these two sets are equal, false otherwise. */
  template <typename TContainer>
  bool eqeq(const TContainer &lhs, const TContainer &rhs) {
    bool result = (lhs.size()==rhs.size());

    for (auto iter = lhs.begin(); result && iter != lhs.end(); ++iter) {
      if (rhs.find(*iter) == rhs.end()) { result = false; }
    }

    return result;
  }

  /* Returns true if these two maps are equal, false otherwise. */
  template <typename TContainer>
  bool eqeq_map(const TContainer &lhs, const TContainer &rhs) {
    bool result = (lhs.size()==rhs.size());

    for (auto iter = lhs.begin(); result && iter != lhs.end(); ++iter) {
      auto pos = rhs.find(iter->first);
      if (pos == rhs.end() || iter->second != pos->second) {
        result = false;
      }
    }

    return result;
  }

}  // Base
