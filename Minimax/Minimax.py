# Jay Kaiser
# jckaiser
# Assignment 3
# 10/12/16

# This assignment was from an Introduction to AI class.
# In such, a Minimax algorithm with Alpha-Beta pruning was created for the game Othello.

# All three of imports were created by my instructor, not by me. I do not claim rights to them.
import copy
import gamePlay
import randomPlay

# nextMove: returns a move given a board state and player color
# input1:   boardState - [[8][8]] list of strings
# input2:   color - 'B' or 'W' depending on player color
# output:   move - a tuple containing (x,y) coordinates of the next move
def nextMove(boardState, color):
    global __color__, __iteration__
    __color__ = color
    __iteration__ = 3
    n, move = max_value(boardState, -1000, 1000, 0)
    return move

# heuristic:    returns a numerical value to describe the given board state
# this heuristic was developed based on Googling for good heuristics, though the code was written myself.
# see the README document for sources to where I got this information.
# though the site where I found this information provided code as well, I voluntarily chose not to look at it.
# coinParity, mobilityIndicator, cornersCaptured, and stabilityIndicator below make up the individual heuristic parts
# input1:       boardState - [[8][8]] list of strings
# input2:       color - a string representing which player is affected by the given boardState
# output:       integer
def heuristic(boardState, color):
    return (
        coinParity(boardState, color) +
        mobilityIndicator(boardState, color) +
        cornersCaptured(boardState, color)
        # + stabilityIndicator(boardState, color)
    )

# coinParity:   part one of the components of the heuristics function
#               determines a value for how many coins each player has control of
# input1:       boardState - [[8][8]] list of strings
# input2:       color - a string representing which player is affected by the given boardState
# output:       integer
def coinParity(boardState, color):
    playerCount = 0
    opponentCount = 0
    for i in boardState:
        for j in i:
            if j != ".":
                if j == color:
                    playerCount += 1
                else:
                    opponentCount += 1
    return 100 * (playerCount - opponentCount) / (playerCount + opponentCount)

# mobilityIndicator:    part two of the components of the heuristics function
#                       determines how many moves the player can make in relation to the other
# input1:               boardState - [[8][8]] list of strings
# input2:               color - a string representing which player is affected by the given boardState
# output:               integer
def mobilityIndicator(boardState, color):
    listOfPlayerMoves, playerTotal = availableMoves(boardState, color)
    if color == "W":
        listOfOpponentMoves, opponentTotal = availableMoves(boardState, "B")
    else:
        listOfOpponentMoves, opponentTotal = availableMoves(boardState, "W")
    if playerTotal + opponentTotal != 0:
        return 100 * (playerTotal - opponentTotal) / (playerTotal + opponentTotal)
    else:
        return 0

# cornersCaptured:  part three of the components of the heuristics function
#                   determines a value based on how many corners the player has control of
# input1:       boardState - [[8][8]] list of strings
# input2:       color - a string representing which player is affected by the given boardState
# output:       integer
def cornersCaptured(boardState, color):
    playerCount = 0
    opponentCount = 0
    cornerA = boardState[0][0]
    cornerB = boardState[0][7]
    cornerC = boardState[7][0]
    cornerD = boardState[7][7]
    listOfCorners = [cornerA, cornerB, cornerC, cornerD]
    for corner in listOfCorners:
        if corner != ".":
            if corner == color:
                playerCount += 1
            else:
                opponentCount += 1
    if playerCount + opponentCount != 0:
        return 100 * (playerCount - opponentCount) / (playerCount + opponentCount)
    else:
        return 0


# stabilityIndicator:   part four of the components of the heuristics function
#                       determines how stable each of the players coins are
# input1:               boardState - [[8][8]] list of strings
# input2:               color - a string representing which player is affected by the given boardState
# output:               integer
def stabilityIndicator(boardState, color):
    playerCount = 0
    opponentCount = 0
    for i in range(8):
        for j in range(8):
            if boardState[i][j] != ".":
                if boardState[i][j] != color:
                    if color != "B":
                        opponentCount += findStability(boardState, i, j, "B")
                    else:
                        opponentCount += findStability(boardState, i, j, "W")
                else:
                    playerCount += findStability(boardState, i, j, color)
    return 100 * (playerCount - opponentCount) / (playerCount + opponentCount)

# findStability:    determines how stable a token at the given coordinates is
# input1:           boardState - a boardState whose stability is tested
# input1:           x - int representing x coordinate of token
# input2:           y - int representing y coordinate of token
# input3:           color - the given color being tested
# output:           1 if determined stable, -1 if determined unstable, and 0 otherwise
def findStability(boardState, x, y, color):
    print "(" + str(x) + "," + str(y) + ")"
    if x == 0:
        if y == 0:
            # NW corner case
            return 1 # corners are always stable
        elif y == 7:
            # NE corner case
            return 1
        else:
            # top edge case
            return horizontalStability(boardState, x, y, color)
    elif x == 7:
        if y == 0:
            # SW corner case
            return 1
        elif y == 7:
            # SE corner case
            return 1
        else:
            # bottom edge case
            return horizontalStability(boardState, x, y, color)
    elif y == 0:
        # left edge case
        return verticalStability(boardState, x, y, color)
    elif y == 7:
        # right edge case
        return verticalStability(boardState, x, y, color)
    else:
        # center of board case
        return min(horizontalStability(boardState, x, y, color),
                   verticalStability(boardState, x, y, color),
                   NWdiagonalStability(boardState, x, y, color),
                   SEdiagonalStability(boardState, x, y, color))

def horizontalStability(boardState, x, y, color):
    if boardState[x][y-1] == color and boardState[x][y+1] == color:
        return 1
    elif boardState[x][y-1] == "." and boardState[x][y+1] == ".":
        return 0
    elif (boardState[x][y-1] == color and boardState[x][y+1] == "." or
                      boardState[x][y - 1] == "." and boardState[x][y + 1] == color):
        return 0
    else:
        return -1

def verticalStability(boardState, x, y, color):
    if boardState[x-1][y] == color and boardState[x+1][y] == color:
        return 1
    elif boardState[x-1][y] == color and boardState[x+1][y] == ".":
        return 0
    elif (boardState[x-1][y] == color and boardState[x+1][y] == "." or
                      boardState[x - 1][y] == "." and boardState[x + 1][y] == color):
        return 0
    else:
        return -1

def SEdiagonalStability(boardState, x, y, color):
    if boardState[x - 1][y-1] == color and boardState[x + 1][y+1] == color:
        return 1
    elif boardState[x - 1][y-1] == color and boardState[x + 1][y+1] == ".":
        return 0
    elif ([x - 1][y-1] == color and boardState[x + 1][y+1] == "." or
                      [x - 1][y - 1] == "." and boardState[x + 1][y + 1] == color):
        return 0
    else:
        return -1

def NWdiagonalStability(boardState, x, y, color):
    if boardState[x-1][y + 1] == color and boardState[x + 1][y - 1] == color:
        return 1
    elif boardState[x-1][y + 1] == color and boardState[x + 1][y - 1] == ".":
        return 0
    elif (boardState[x-1][y + 1] == color and boardState[x + 1][y - 1] == "." or
                      boardState[x - 1][y + 1] == "." and boardState[x + 1][y - 1] == color):
        return 0
    else:
        return -1

# max_value: completes the max portion of Minimax, with regard to AlphaBeta Pruning
# input1:   boardState - a given boardState to be judged through alphaMax
# input2:   alpha - a minimum value to be compared to
# input3:   beta - a maximum value to be compared to
# input4:   iteration - an int representing what level of Minimax we're in
# output1:  v - an int representing the value of the best move to be made
# (output2:  move - a tuple containing the (x,y) coordinates of said best move)
# yes, I know it is bad practice to have two different returns, but it was the best way I could do it
def max_value(boardState, alpha, beta, iteration):
    v = -1000
    listOfMoves, total = availableMoves(boardState, __color__)
    if total == 0 and iteration == 0:
        return v, "pass" # in the case that there are no valid moves in the initial iteration
    elif total == 0:
        return 1000 # if there are no moves otherwise
    for m in listOfMoves:
        n = value(boardState, m, alpha, beta, iteration + 1, "min")
        v = max(v, n)
        if v == n:
            bestMove = m
        if v >= beta and iteration == 0:
            return v, bestMove # return move only in initial iteration
        elif v >= beta:
            return v
        alpha = max(alpha, v)
    if iteration == 0:
        return v, bestMove # return move only in initial iteration
    else:
        return v

# min_value: completes the min portion of Minimax, with regard to AlphaBeta Pruning
# input1:   boardState - a given boardState to be judged through alphaMax
# input2:   alpha - a minimum value to be compared to
# input3:   beta - a maximum value to be compared to
# input4:   iteration - an int representing what level of Minimax we're in
# output1:  v - an int representing the value of the best move to be made
# output2:  move - a tuple containing the (x,y) coordinates of said best move
def min_value(boardState, alpha, beta, iteration):
    v = 1000
    listOfMoves, total = availableMoves(boardState, __color__)
    if total == 0:
        return -1000
    for m in listOfMoves:
        n = value(boardState, m, alpha, beta, iteration + 1, "max")
        v = min(v, n)
        if v <= alpha:
            return v
        beta = min(beta, v)
    return v

# value:    returns the value of a given Minimax iteration
# input1:   boardState - a given boardState with the new move appended onto it
# input2:   move - a given move to be applied to the boardState and judged through alphaMax
# input3:   alpha - a minimum value to be compared to
# input4:   beta - a maximum value to be compared to
# input5:   iteration - an int representing what level of Minimax we're in
#           an iteration of 5 is too high, and takes almost a minute a solve
# input6:   nextState - a string (either "min" or "max") representing who's turn is next
# output1:  integer representing the heuristic value of the move
# output2:  move - a tuple containing the (x,y) coordinates of said best move
def value(boardState, move, alpha, beta, iteration, nextState):
    testBoard = copy.deepcopy(boardState)
    gamePlay.doMove(testBoard, __color__, move)
    if iteration == __iteration__: # reaching a max recursion of 2 for now.
        return heuristic(testBoard, __color__)
    else:
        if nextState == "max":
            return max_value(testBoard, alpha, beta, iteration)
        else:
            return min_value(testBoard, alpha, beta, iteration)

# availableMoves:   determines the possible moves that can be made by color on a given board
# input:            boardState - a given boardState from which new moves will be determined
# output1:          listOfMoves - a list of tuples representing possible moves that can be made by color
# output2:          total - the number of moves in listOfMoves
def availableMoves(boardState, color):
    listOfMoves = []
    total = 0
    for i in range(8):
        for j in range(8):
            if gamePlay.valid(boardState, color, (i,j)):
                listOfMoves.append((i,j))
                total += 1
    if total == 0:
        return "pass", 0
    else:
        return listOfMoves, total
