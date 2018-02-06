package bufmgr;

import global.PageId;

public class FrameDesc {
    protected int PinCount;
    protected boolean DirtyBit;
    protected boolean ValidData;
    protected PageId pageNum;
    protected int FrameNum;
    protected int ReferenceBit;

    public FrameDesc() {
        this.PinCount = 0;
        this.DirtyBit = false;
        this.pageNum = new PageId();
        this.ValidData = false;
        this.ReferenceBit = 0;
    }

    public void incrementPinCount(){
        this.PinCount += 1;
    }
    public void decrementPinCount(){
        if(this.PinCount > 0)
        {
            this.PinCount -= 1;
        }
    }
    public void setFrameNum(int framenum)
    {
        this.FrameNum = framenum;
    }
    public void setPageNum(PageId pagenum){
        this.pageNum.copyPageId(pagenum);
    }

    public void setDirtyBit(boolean dirtyBit) {
        DirtyBit = dirtyBit;
    }

    public int getPinCount() {
        return PinCount;
    }

    public int getReferenceBit() {
        return ReferenceBit;
    }

    public void setReferenceBit(int referenceBit) {
        ReferenceBit = referenceBit;
    }

    public boolean isDirty() {
        return DirtyBit;
    }

    public void setValidBit(boolean validData) {
        ValidData = validData;
    }

    public int getFrameNum() {

        return FrameNum;
    }

    public boolean isValid(){
        return this.ValidData;

    }

    public void setPinCount(int pins){
        this.PinCount = pins;
    }

    public void setPageNo(PageId toCopy){
        this.pageNum.copyPageId(toCopy);
    }
}
