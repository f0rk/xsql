def get_quote_positions(data, allow_dollar_quoting=True):

    idx = 0
    chars = list(data)

    quote_positions = []

    in_dollar_quote = False
    in_regular_quote = False
    quote = None
    quote_open_idx = None

    while idx < len(chars):

        c = chars[idx]

        if in_dollar_quote:
            if c == "$":
                maybe_close = c
                m_idx = idx + 1
                while m_idx < len(chars):
                    m = chars[m_idx]
                    maybe_close += m
                    m_idx += 1
                    if m == "$":
                        break

                if maybe_close == quote:

                    quote_positions.append((quote_open_idx, m_idx))
                    quote_open_idx = None

                    # print("dollar quote {} closed at {}".format(quote, idx))
                    # show_context(idx)

                    idx = m_idx
                    in_dollar_quote = False
                    quote = None

        elif in_regular_quote:
            if c == quote:
                try:
                    next_c = chars[idx+1]
                    # escaped
                    if c == next_c:
                        idx += 1
                    else:
                        # print("regular quote {} closed at {}".format(quote, idx))
                        # show_context(idx)

                        quote_positions.append((quote_open_idx, idx))
                        quote_open_idx = None

                        in_regular_quote = False
                        quote = None
                except IndexError:
                    pass

        else:
            if c == "$" and allow_dollar_quoting:
                quote = c
                n_idx = idx + 1
                while n_idx < len(chars):
                    n = chars[n_idx]
                    quote += n
                    n_idx += 1
                    if n == "$":
                        break

                in_dollar_quote = True
                quote_open_idx = idx

                # print("dollar quote {} opened at {}".format(quote, idx))
                # show_context(idx)

                idx = n_idx

            elif c in ('"', "'", "`"):
                quote = c
                in_regular_quote = True
                quote_open_idx = idx

                # print("regular quote {} opened at {}".format(quote, idx))
                # show_context(idx)

        idx += 1

    return quote_positions


def is_in_quote(quote_positions, idx):
    for quote_start, quote_end in quote_positions:
        if quote_start <= idx <= quote_end:
            return True
    return False
