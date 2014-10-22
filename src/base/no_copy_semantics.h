/* <base/no_copy_semantics.h>

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

   Defines a macro to disable compiler-provided copy semantics.
 */

#pragma once

/* Use this macro to disable compiler-provided copy semantics in any
   class you intend to operate as a reference type.  Convention is to
   have this macro appear as the first thing in the class, before the
   first accessability declaration, like this:

      class TFoo {
        NO_COPY_SEMANTICS(TFoo);
        public:
        ...
      }

   If you attempt to copy-construct or assign an instance of a class
   declared with this macro, you'll get a compile-time error telling you
   that the copy constructor or assignment operator is private.  If the
   attempt is made from within a scope friendly to the class, you'll get
   a link-time error telling you the copy constructor or assignment
   operator is undefined. */
#define NO_COPY_SEMANTICS(cls) \
  cls(const cls &) = delete; \
  cls &operator=(const cls &) = delete;
