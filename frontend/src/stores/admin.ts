import { create } from 'zustand';
import { persist, createJSONStorage } from 'zustand/middleware';

interface AdminState {
  adminKey: string;
  isAuthenticated: boolean;
  setAdminKey: (key: string) => void;
  setAuthenticated: (auth: boolean) => void;
  getHeaders: () => Record<string, string>;
}

export const useAdminStore = create<AdminState>()(
  persist(
    (set, get) => ({
      adminKey: '',
      isAuthenticated: false,
      setAdminKey: (key: string) => set({ adminKey: key }),
      setAuthenticated: (auth: boolean) => set({ isAuthenticated: auth }),
      getHeaders: () => {
        const key = get().adminKey;
        return key ? { 'X-Admin-Key': key } : {} as Record<string, string>;
      },
    }),
    {
      name: 'arena-admin',
      storage: createJSONStorage(() => sessionStorage),
      partialize: (state) => ({ adminKey: state.adminKey, isAuthenticated: state.isAuthenticated }),
    }
  )
);
