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
        int victim = -1;
        for(int counter = 0; counter < poolSize*2; counter++) //loop through entire array up to two times
        {
            int pincnt = frametable[currentFrame].getPinCount();
            if (pincnt < 1) {
                if (!frametable[currentFrame].isValid()) {//if data is not valid, kick it out
                    victim = currentFrame;
                    currentFrame = (currentFrame + 1) % poolSize;
                    return victim;
                }
                if (frametable[currentFrame].getReferenceBit() == 1) {
                    frametable[currentFrame].setReferenceBit(0);
                } else {
                    victim = currentFrame;
                    currentFrame = (currentFrame + 1) % poolSize;
                    return victim;
                }
            }
            currentFrame = (currentFrame + 1) % poolSize;
        }
        return victim; //No frame found to replace
    }
}
