/* <base/no_construction.h>

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

   Defines a macro to disable construction of an aggregate type.
 */

#pragma once

/* Use this macro to disable construction of a class, struct, or union,
   essentially turning the type into a namespace with accessibility
   controls.  This is particularly useful for creating namespace-like
   separation of declarations within classes.  For example:

      class TFoo {
        public:

        struct Alpha {
          NO_CONSTRUCTION(Alpha);
          static CallMe();
        };

        struct Beta {
          NO_CONSTRUCTION(Beta);
          static CallMe();
        };

      };

   A caller can now call TFoo::Alpha:CallMe() or TFoo::Beta::CallMe() without
   ambiguity.  Note that it is convention *not* to use the T-prefix with types
   without construction.  This makes them read more like namespaces, which they
   essentially are.

   All the members of a type without construction must obviously be static.  A
   per-instance member makes no sense in a type that will never be
   instantiated.

   If you attempt to default-construct, destruct, copy-construct or assign an
   instance of a type declared with this macro, you'll get a compile-time error
   telling you function is private.  If the attempt is made from within a scope
   friendly to the type, you'll get a link-time error telling you the function
   is undefined. */
#define NO_CONSTRUCTION(cls) \
  cls() = delete; \
  ~cls() = delete; \
  cls(const cls &) = delete; \
  cls &operator=(const cls &) = delete;
