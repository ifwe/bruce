/* <base/convert.h>

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

   Converts pieces to values.
 */

#pragma once

#include <base/opt.h>
#include <base/piece.h>
#include <base/syntax_error.h>

//TODO: Move stuff to CC.
namespace Base {

  class TConverter {
    public:

    TConverter(const TPiece<const char> &buf) : Working(buf) {}

    /* Reads a sign (+/-) From the input stream. If + is read, then it returns
       true, if - is read, then it returns false. If some other character is
       the next character at the head of the input stream, it returns
       TOpt::Unknown. */
    TOpt<bool> TryReadSign() {
      assert(this);

      if(!(*this)) {
        return  *TOpt<bool>::Unknown;
      }

      bool ret;

      if (*(*this) == '+') {
         ret = true;
      } else if (*(*this) == '-') {
        ret = false;
      } else {
        return *TOpt<bool>::Unknown;
      }

      ++(*this);
      return ret;
    }

    /* TryReadSign wrapper that asserts that a sign is read or throws. */
    bool ReadSign() {
      assert(this);
      TOpt<bool> ret = TryReadSign();

      if (ret) {
        return *ret;
      }

      throw TSyntaxError(HERE,  "Expected sign (+/-) but didn't find one.");
    }

    /* Returns true if there is a next character in the input stream and it is
       a digit, false otherwise. */
    bool HasDigit() {
      assert(this);
      return ((*this) && isdigit(*(*this)));
    }

    void ConsumeWhitespace() {
      assert(this);
      while(isspace(**this)) { ++(*this); }
    }

    /* Reads in a character from Input and converts it to it's decimal value.
     */
    template<typename TVal> bool TryReadDigit(TVal &output) {
      assert(this);
      assert(&output);
      assert(std::numeric_limits<TVal>::is_exact);
      assert(std::numeric_limits<TVal>::min() <= 0);
      assert(std::numeric_limits<TVal>::max() >= 9);

      if (!HasDigit()) {
        return false;
      }

      char c = *(*this);
      ++(*this);

      output = (c - '0');
      return true;
    }

    /* Reads in an unsigned integer and stores it inside output. This operation
       is automic. The positive flag changes whether each digit is added or
       subtracted from the value to make the end result positive/negative. This
       is useful when the sign is known in advance so we can get more precise
       bounds checking. */
    template<typename TVal> bool TryReadUnsignedInt(TVal &output,
        bool positive = true) {
      assert(this);
      assert(&output);
      assert(std::numeric_limits<TVal>::is_integer);
      assert(std::numeric_limits<TVal>::is_exact);
      assert(positive || std::numeric_limits<TVal>::is_signed);
      ConsumeWhitespace();

      if(!HasDigit()) {
        return false;
      }

      TVal digit_val;
      TVal cur_val = 0;
      const char *err_msg=0;

      while (TryReadDigit(digit_val)) {
        // Check for overflow/underflow
        if (!err_msg) {
          if (positive) {
            if ((cur_val > std::numeric_limits<TVal>::max() / 10) ||
                (std::numeric_limits<TVal>::max() - cur_val * 10) <
                 digit_val) {
              err_msg = "Int overflowed type bounds.";
            }
          } else {
            // Digit is negative, since number is negative.
            if ((cur_val < std::numeric_limits<TVal>::min() / 10) ||
                (std::numeric_limits<TVal>::min() - cur_val * 10) >
                 digit_val) {
              err_msg = "Int underflowed type bounds.";
            }
          }

          // Add to value
          cur_val *= 10;

          if (positive) {
            cur_val += digit_val;
          } else {
            cur_val -= digit_val;
          }
        }
      }

      if (err_msg) {
        throw TSyntaxError(HERE,err_msg);
      }

      output = cur_val;
      return true;
    }

    /* Wrapper for TryReadUnsignedInt which asserts that an UnsignedInt must be
       read or an exception thrown. */
    template <typename TVal>
    TConverter &ReadUnsignedInt(TVal &output, bool positive=true) {
      assert(this);

      if (!TryReadUnsignedInt(output, positive)) {
        throw TSyntaxError(HERE,
            "No integer exists at current location in stream.");
      }

      return *this;
    }

    /* Parses both signed and unsigned integers from a readable stream
       performing bounds checking. If it succeeds, it returns true. If it fails
       without changing the input state it returns false. If try_read is false,
       it throws an error on invalid with a message about the first thign to go
       wrong. No matter what it finishes reading all digits that belong to the
       integer before returning false or throwing an error. Note that you
       should not call TryReadInt with the try_read flag set manually, rather
       call ReadInt if you mean try_read=false. At the moment this only
       supports decimal representations, but in the future will be expanded to
       support hexadecimal and octal. */
    template <typename TVal>
    bool TryReadInt(TVal &output, bool sign_required = false) {
      assert(this);
      assert(&output);
      assert(std::numeric_limits<TVal>::is_integer);
      assert(std::numeric_limits<TVal>::is_exact);
      assert(sign_required ? std::numeric_limits<TVal>::is_signed : true);

      /* TODO: Hex, octal, etc. support. */
      /* TODO: Format strings support. */

      // if signed, first character may be +/- sign
      TOpt<bool> positive_opt = TryReadSign();

      if (positive_opt) {
        if (!TryReadUnsignedInt(output, *positive_opt)) {
          throw TSyntaxError(HERE,
              "Consumed a sign, but didn't find any digits after it.");
        } else {
          return true;
        }
      } else if (sign_required) {
        return false;
      }

      /* No sign was specified, so we must be positive. Read the digits if we
         can. If we can't, we were only trying. */
      return TryReadUnsignedInt(output, true);
    }

    /* Wrapper for TryReadSignedInt which asserts that if no integer was found
       at the current stream head, then an exception should be thrown. */
    template <typename TVal>
    TConverter &ReadInt(TVal &output, bool sign_required = false) {
      assert(this);

      if (!TryReadInt(output, sign_required)) {
        throw TSyntaxError(HERE, "No integer exists at current location.");
      }

      return *this;
    }

    /* Parse bool */
    bool TryReadBool(bool &output);
    void ReadBool(bool &output);

    /* Parse float */
    bool TryReadFloat(float &output, bool sign_required=false);
    void ReadFloat(float &output, bool sign_required=false);

    /* Parse double */
    bool TryReadDouble(double &output, bool sign_required=false);
    void ReadDouble(double &output, bool sign_required=false);

    operator bool() const {
      assert(this);
      return Working;
    }

    char operator*() const {
      assert(Working);
      return Working.GetHead();
    }

    void Read(int &val) {
      ReadInt(val);
    }

    void Read(long &val) {
      ReadInt(val);
    }

    void Read(unsigned &val) {
      ReadUnsignedInt(val);
    }

    void Read(size_t &val) {
      ReadUnsignedInt(val);
    }

    TConverter &operator++() {
      assert(this);

      if (!Working) {
        throw TSyntaxError(HERE, "unexpectedly out of text");
      }

      Working.AdjustStart(1);
      return *this;
    }

    private:
    TPiece<const char> Working;
  };

  class TConvertProxy {
    NO_COPY_SEMANTICS(TConvertProxy);

    public:
    TConvertProxy(const TPiece<const char> text) : Converter(text) {
    }

    template <typename TVal>
    operator TVal() {
      TVal val;
      Converter.Read(val);

      if (Converter) {
        // TODO: Stringify TVal.
        throw TSyntaxError(HERE, "Extra junk after value.");
      }

      return val;
    }

    private:
    TConverter Converter;
  };
}
