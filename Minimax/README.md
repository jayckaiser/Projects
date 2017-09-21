Jay Kaiser
jckaiser
10/12/16

This program can only go until an iteration of 4 (MAX MIN MAX MIN) reliably. Past that, it takes nearly a minute per game completion on my computer. While there are ways to allow deeper traversal at a faster speed (using wiser search methods), I'm happy with how my program works as is.

(The given iteration value is set to be 3, as my code will always beat simpleGreedy and randomPlay at that level of depth, and it takes a very small amount of time to finish on my computer. To change the level of iteration, simply change __iteration__ in hw3.nextMove() to the level of iteration wanted.)

(There is currently a bug in my code where if the game is played at a depth of 4 against simpleGreedy and hw3 goes first, simpleGreedy will nearly always win. I don't know what is causing this bug, but it's so particular that I left it as is. This is also part of the reason why I chose the given iteration of depth to end at 3.)


For the heuristic, I was unsure what would be best (especially because I didn't know how to play othello anyway), so a quick google search gave me some links in the right direction for best heuristics to use. Below are posted sources from where I found my information. Regardless of the source where I found the idea for a heuristic, I did not look at any code, only pseudocode for the idea.

https://kartikkukreja.wordpress.com/2013/03/30/heuristic-function-for-reversiothello/
http://stackoverflow.com/questions/13314288/need-heuristic-function-for-reversiothello-ideas

I divided my heuristic into four parts:

coinParity: compared how many coins each player had, with more coins indicating a better value

mobilityIndicator: compared how many possible moves each player had, with more moves indicating a better value

cornersCaptured: compared how many corners each player had captured, as corners are stable and unchanging, regardless of how gameplay progressed, providing the player with them special benefits

stabilityIndicator: compared how stable each players' tokens were in their given position, choosing between stable, unstable, and semi-stable, with point values of 1, -1, and 0 respectively.

I ended up not using stabilityIndicator for 2 reasons:
a) I couldn't get it work as expected.
b) I didn't fully understand it.

Therefore, its remnants have been left in my code, though they are not used in the final heuristic. Despite this, the heuristic as is works fine; I acknowledge that in the case of play against a human, this would have to be fine-tuned to work as well, however.