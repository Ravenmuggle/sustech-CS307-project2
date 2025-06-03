package edu.sustech.cs307.storage;

import java.util.*;

public class LRUReplacer {

    private final int maxSize;
    private final Set<Integer> pinnedFrames = new HashSet<>();
    private final Set<Integer> LRUHash = new HashSet<>();
    private final LinkedList<Integer> LRUList = new LinkedList<>();

    public LRUReplacer(int numPages) {
        this.maxSize = numPages;
    }

    public int Victim() {
        if(LRUHash.isEmpty()){
            return -1;
        }
        else {
            int LRU=LRUList.getFirst();
            LRUList.removeFirstOccurrence(LRU);
            LRUHash.remove(LRU);
            return LRU;
        }
    }

    public int Victim2() {
        int victim = Victim();
        if (victim != -1) return victim;
        for (int i = 0; i < maxSize; i++) {
            if(!pinnedFrames.contains(i)){
                return i;
            }
        }
        return -1;
    }

    public void remove(int LRU) {
        LRUList.removeFirstOccurrence(LRU);
        LRUHash.remove(LRU);
    }

    public void Pin(int frameId) {
        if (pinnedFrames.contains(frameId)) {
            //doing nothing
        } else if (LRUHash.contains(frameId)) {
            LRUHash.remove(frameId);
            LRUList.removeFirstOccurrence(frameId);
            pinnedFrames.add(frameId);
        } else {
            if (size()+1 > maxSize) {
                throw new RuntimeException("REPLACER IS FULL");
            }
            else {
                pinnedFrames.add(frameId);
            }
        }
    }


    public void Unpin(int frameId) throws RuntimeException {
        if (pinnedFrames.contains(frameId)) {
            pinnedFrames.remove(frameId);
            LRUHash.add(frameId);
            LRUList.addLast(frameId);
        }
        else{
            throw new RuntimeException("UNPIN PAGE NOT FOUND");
        }
    }


    public int size() {
        return LRUList.size() + pinnedFrames.size();
    }
}