package bufmgr;


import global.Page;

/*
Jason Larson, CS 487
This class implements the Clock replacement policy algorithm for the buffer manager.

 */
public class Clock {
    int currentFrame;

    public Clock() {
        currentFrame = 0; //start at first frame for the first iteration
    }

    public int selectVictim(FrameDesc[] frametable, int poolSize){
        for(int counter = 0; counter < poolSize*2; counter++) //loop through entire array up to two times
        {
            if(!frametable[currentFrame].isValid()){//if data is not valid, kick it out
                return currentFrame;
            }
            if(frametable[currentFrame].getPinCount() == 0) {
                if (frametable[currentFrame].getReferenceBit() == 1) {
                    frametable[currentFrame].setReferenceBit(0);
                }
                else {
                    return currentFrame;
                }
            }
            currentFrame = (currentFrame + 1) % poolSize;
        }
        return -1; //No frame found to replace
    }
}
