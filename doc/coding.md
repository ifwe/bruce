## Coding Conventions

In general, the coding convention for Bruce follows the
[Google C++ Style Guide](http://google-styleguide.googlecode.com/svn/trunk/cppguide.xml)

### Indentation

* Use two spaces for indentation.
* Please use spaces rather than tab characters.
* Please do not have trailing whitespace.
* Empty lines are **empty**, i.e.: contain no space characters.
* Wrap lines at 79 characters.  If you must wrap a line, please intent
it **two** levels from where the subsequent line begins.

### Naming and Namespaces

When you use a namespace, please indent its contents one level from the
surrounding code.

When closing a curly brace, follow it with a `//` comment declaring which
namespace has just been closed. For example:

```C++
namespace Foo {

 class TBar {
   // members go here
 };  // TBar

}  // Foo
```

### Local variables

All local variables are lowercased. Spaces are represented by an underscore
(`_`).

### Member variables

All member variables use title casing. (ThisIsAMemberVariable)

If something is abbreviated, so title case makes it look like a macro because
it's all caps, capitalize the first letter only:

"TASM" should be "TAsm"

### Pointers

Attach the `*` for pointers to whatever is on the right hand side. E.g.:

```C++
const char *foo;
```

### Type names

All type names start with a capital `T`. These use title casing (e.g.
`TCompilerConfig`).

### Curly braces

#### General rules

* Please place the open brace on the same line as the statement which begins
the block.
* Please place the closed brace on a new line at the end of the block.
* Braces should be indented to the same level as the thing to which they're
attached.

#### With conditionals

* An `if` block should still have curly braces if it's only one line.
* For an `else` block, the closing and opening braces should be placed directly on either side of the `else`, separated from it by a single space.

### Parentheses

* Please use a space between an `if` and the `(` of the conditional.
* Please use a space between the `)` of a conditional and the opening bracket
of the block which follows.
* Please use parentheses around grouping expressions if there is a complex
expression in the conditional.
* Please do not put a space between the function name and the opening
parenthesis.

### Classes

#### General rules

* In general, prefer aggregation to deep class hierarchies.
* Class definitions should be indented one level from the surrounding code.
* Classes should be as self-contained as possible.
* A Class should initialize its internal state using a member initializer list
on the constructor.
* All implementation details of the class should be private, and it should be
explicitly specify how to interact with the class.
* Classes should not be able to enter a "bad state."  Please follow the
[RAII](http://en.wikipedia.org/wiki/Resource_Acquisition_Is_Initialization)
idiom.
* Throw in constructors to indicate errors.
* Never throw in a destructor.
* Use the `NO_CONSTRUCTION` and `NO_COPY_SEMANTICS` macros whenever necessary
(or = delete).
* Base classes must have virtual destructors.
* Use '= default' when possible to delegate to standard
constructors/destructors which would otherwise be deleted.

#### Declaration order

* Public things should be declared first, followed by protected, and then
private.

#### Functions and methods

* Follow good
[const correctness](http://en.wikipedia.org/wiki/Const-correctness) practices.

### Includes

#### General rules

* Please do not put a `using NAMESPACE` before the includes.
* All includes should be placed within angle brackets: `< >`
* Please do not include unnecessarily.  Clean up your includes if you no longer
need them.
* Please try to move the includes to the .cc file instead of headers wherever
possible.

#### Ordering

* Includes should be grouped as follows:
    1. corresponding header file if this is a .cc file
    2. standard library includes (for instance, `<vector>`)
    1. system includes (for instance, `<signal.h>`)
    1. program-specific includes
* Within each grouping, the includes should be listed alphabetically.
* There should be a blank line between each of these groupings.

### Comments

#### General rules

* Inline documentation should be done using `//`.
* Comment your code well.

#### Top of every file

At the top of every file there should be a large comment which includes:

* The name of the file
* The license statement
* The copyright statement
* A description of what the class does.

If the file is a .cc, please put this comment into its .h file.

-----

coding.md: Copyright 2014 if(we), Inc.

coding.md is licensed under a Creative Commons Attribution-ShareAlike 4.0
International License.

You should have received a copy of the license along with this work. If not,
see <http://creativecommons.org/licenses/by-sa/4.0/>.
