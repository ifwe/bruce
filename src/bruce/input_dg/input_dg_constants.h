/* <bruce/input_dg/input_dg_constants.h>

   ----------------------------------------------------------------------------
   Copyright 2013-2014 if(we)

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

   Common constants for input datagram format.
 */

#pragma once

/* It should be possible to compile everything in here with a C compiler.
   That's why there are no namespaces below. */

enum { INPUT_DG_SZ_FIELD_SIZE = 4 };

enum { INPUT_DG_API_KEY_FIELD_SIZE = 2 };

enum { INPUT_DG_API_VERSION_FIELD_SIZE = 2 };

/* TODO: remove this */
enum { INPUT_DG_OLD_VER_FIELD_SIZE = 1 };
