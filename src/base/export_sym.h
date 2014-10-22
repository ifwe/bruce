/* <base/export_sym.h>

   ----------------------------------------------------------------------------
   Copyright 2014 if(we)

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

   Preprocessor macro for marking library symbols as public.
 */

#pragma once

/* If you have a function foo() that is part of a library's public interface,
   then mark it as public as follows:

       void EXPORT_SYM foo() {
         // implementation of foo()
       }

   You should then compile the library's source files with the
   -fvisibility=hidden option specified.  Then only the symbols marked with
   EXPORT_SYM will be accessible to users of the library.
 */
#define EXPORT_SYM __attribute__((__visibility__("default")))
