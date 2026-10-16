---
description:
  Mark this session done in Hyperspace, with an optional outcome note
argument-hint: "[outcome note]"
allowed-tools: Bash(hyperspace done:*)
---

The user marks this session done. Their note, which can be empty, is:

<note>$ARGUMENTS</note>

Run this command once, with the note as one shell-quoted argument (leave out
`--note` if the note is empty):

```
hyperspace done --by-user --note '<note>'
```

Then report the result in one line. Do not start new work. The session stays
open: the user can keep talking to you, and their next prompt reopens the
session.
